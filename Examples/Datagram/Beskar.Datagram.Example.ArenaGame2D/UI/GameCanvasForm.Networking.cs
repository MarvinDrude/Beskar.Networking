using System.Diagnostics;
using System.Text;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Datagram.Example.ArenaGame2D.UI;

public sealed partial class GameCanvasForm
{
   private async Task InitializeNetworkingAsync()
   {
      if (_isInitialized) return;
      _isInitialized = true;

      var opts = GameProtocol2D.CreateOptions();

      if (_isHost)
      {
         _datagramHost = new DatagramHost(new System.Net.IPEndPoint(System.Net.IPAddress.Any, _targetEndPoint.Port), opts, _transport);
         _datagramHost.Events.ValidateConnection = ctx =>
         {
            var rawName = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span).Trim();
            if (string.IsNullOrWhiteSpace(rawName)) rawName = $"Pilot{Random.Shared.Next(10, 99)}";
            ctx.PeerTag = rawName.Length > 12 ? rawName[..12] : rawName;
            return ValueTask.CompletedTask;
         };

         _datagramHost.Start();

         var boundPort = _datagramHost.LocalEndPoint?.Port ?? _targetEndPoint.Port;
         AddFeed($"Hosting on UDP :{boundPort} (AEAD + PSK).");
      }
      else
      {
         _datagramHost = new DatagramHost(new System.Net.IPEndPoint(System.Net.IPAddress.Any, 0), opts, _transport);
         _datagramHost.Start();

         try
         {
            var connectTask = _datagramHost.ConnectAsync(_targetEndPoint, Encoding.UTF8.GetBytes(_playerName));
            while (!connectTask.IsCompleted)
            {
               _datagramHost.Tick();
               await Task.Delay(15);
            }

            _serverPeer = await connectTask;
            AddFeed($"Connected to {_targetEndPoint}!");
         }
         catch (Exception ex)
         {
            _connected = false;
            _statusBanner = $"Could not connect to {_targetEndPoint}: {ex.Message}";
            AddFeed(_statusBanner);
            _readyTcs.TrySetResult();
            Invalidate();
            return;
         }
      }

      UiTheme.BeginHighResTimer();
      _highResTimerActive = true;
      _lastFrameTimeSec = _clock.Elapsed.TotalSeconds;
      _frameTimer.Tick += OnFrameTimerTick;
      _frameTimer.Start();
      _readyTcs.TrySetResult();
   }

   private async Task ShutdownNetworkingAsync()
   {
      _frameTimer.Stop();

      if (_highResTimerActive)
      {
         UiTheme.EndHighResTimer();
         _highResTimerActive = false;
      }

      if (_datagramHost is not null)
      {
         if (_isHost)
         {
            foreach (var p in _datagramHost.Peers)
            {
               p.Disconnect("Host closed arena"u8);
            }
            _datagramHost.Tick();
         }
         else if (_serverPeer is not null && _serverPeer.State == DatagramPeerState.Connected)
         {
            _serverPeer.Disconnect("Client quit"u8);
            _datagramHost.Tick();
         }

         await Task.Delay(30);
         await _datagramHost.DisposeAsync();
         _datagramHost = null;
      }

      _fonts.Dispose();
   }

   private void StepHostTick()
   {
      var host = _datagramHost!;
      var sim = _sim!;
      var local = _hostLocalPlayer!;

      host.Tick();
      PollHostEvents(host, sim);

      var (buttons, aim) = SampleCurrentControls(local.X, local.Y);
      sim.ApplyLocalHostInput(local, buttons, aim);
      sim.Step();

      FlushHostBroadcasts(host, sim);

      if ((sim.Tick & 1) == 0 && host.PeerCount > 0)
      {
         BroadcastHostSnapshot(host, sim);
      }

      host.Tick();
   }

   private void PollHostEvents(DatagramHost host, Simulation.ArenaSimulation2D sim)
   {
      host.PollEvents(evt =>
      {
         switch (evt.Type)
         {
            case DatagramEventType.PeerConnected:
            {
               var name = evt.Peer.Tag as string ?? $"Pilot{evt.Peer.PeerId}";
               var entity = sim.AddPlayer(name, evt.Peer.PeerId);
               var wLen = GameProtocol2D.WriteWelcome(_packetBuf, entity.Id, entity.Color, entity.X, entity.Y);
               evt.Peer.TrySend(GameProtocol2D.ChLifecycle, _packetBuf.AsSpan(0, wLen));
               sim.ScoreboardDirty = true;
               break;
            }

            case DatagramEventType.PeerDisconnected:
               sim.RemovePlayerByPeer(evt.Peer.PeerId, evt.DisconnectReason.ToString());
               break;

            case DatagramEventType.MessageReceived:
            {
               var player = sim.FindByPeer(evt.Peer.PeerId);
               if (player is null) break;

               if (evt.ChannelId == GameProtocol2D.ChRealtime)
               {
                  Span<ClientInputFrame> inputScratch = stackalloc ClientInputFrame[GameProtocol2D.MaxRedundantInputs];
                  var count = GameProtocol2D.ReadInputBundle(evt.Payload.Span, inputScratch);
                  if (count > 0) sim.ReceiveClientInputBundle(player, inputScratch[..count]);
               }
               else if (evt.ChannelId == GameProtocol2D.ChLifecycle
                        && GameProtocol2D.TryReadEmote(evt.Payload.Span, out var emoteIdx))
               {
                  var emoteText = GameProtocol2D.Emotes[emoteIdx % GameProtocol2D.Emotes.Length];
                  BroadcastEvent($"{player.Name}: \"{emoteText}\"");
               }
               break;
            }
         }
      });
   }

   private void FlushHostBroadcasts(DatagramHost host, Simulation.ArenaSimulation2D sim)
   {
      foreach (var ev in sim.PendingEvents)
      {
         BroadcastEvent(ev);
      }
      sim.PendingEvents.Clear();

      foreach (var fx in sim.PendingFx)
      {
         SpawnFxVisuals(fx);
         var fxLen = GameProtocol2D.WriteFxCue(_packetBuf, fx.X, fx.Y, fx.Kind, fx.Color);
         host.Broadcast(GameProtocol2D.ChFx, _packetBuf.AsSpan(0, fxLen));
      }
      sim.PendingFx.Clear();

      sim.FillScoreboard(_scoreboard);
      UpdateNameMap();

      if (sim.ScoreboardDirty)
      {
         sim.ScoreboardDirty = false;
         var sbLen = GameProtocol2D.WriteScoreboard(_packetBuf, _scoreboard);
         host.Broadcast(GameProtocol2D.ChScoreboard, _packetBuf.AsSpan(0, sbLen));
      }
   }

   private void BroadcastHostSnapshot(DatagramHost host, Simulation.ArenaSimulation2D sim)
   {
      sim.FillSnapshotLists(_renderPlayers, _renderOrbs, _renderLasers);
      var snapLen = GameProtocol2D.WriteWorldSnapshot(_packetBuf, sim.Tick, 0, _renderPlayers, _renderOrbs, _renderLasers);

      foreach (var peer in host.Peers)
      {
         if (peer.State != DatagramPeerState.Connected) continue;

         var pEntity = sim.FindByPeer(peer.PeerId);
         GameProtocol2D.PatchSnapshotAckedInput(_packetBuf, pEntity?.LastProcessedInputSeq ?? 0);
         peer.TrySend(GameProtocol2D.ChRealtime, _packetBuf.AsSpan(0, snapLen));
      }
   }

   private void StepClientTick(double nowSec)
   {
      var client = _datagramHost!;
      var peer = _serverPeer!;

      var (buttons, aim) = SampleCurrentControls(_netcode.RenderLocalX, _netcode.RenderLocalY);

      Span<ClientInputFrame> bundle = stackalloc ClientInputFrame[GameProtocol2D.MaxRedundantInputs];
      var count = _netcode.RecordAndPredictInput(buttons, aim, bundle, nowSec, (fx, orbVal) => SpawnFxVisuals(fx, orbVal));

      var inLen = GameProtocol2D.WriteInputBundle(_packetBuf, bundle[..count]);
      peer.TrySend(GameProtocol2D.ChRealtime, _packetBuf.AsSpan(0, inLen));

      client.Tick();
      PollClientEvents(client, nowSec);

      if ((_netcode.LatestServerTick & 1) == 0)
      {
         _netcode.RecordTelemetrySample((float)peer.Stats.SmoothedRttMs);
      }
   }

   private void PollClientEvents(DatagramHost client, double nowSec)
   {
      client.PollEvents(evt =>
      {
         switch (evt.Type)
         {
            case DatagramEventType.PeerDisconnected:
               _connected = false;
               var reason = evt.Payload.Length > 0 ? Encoding.UTF8.GetString(evt.Payload.Span) : evt.DisconnectReason.ToString();
               _statusBanner = $"Disconnected: {reason}";
               AddFeed(_statusBanner);
               break;

            case DatagramEventType.MessageReceived:
               HandleClientMessage(evt, nowSec);
               break;
         }
      });
   }

   private void HandleClientMessage(DatagramEvent evt, double nowSec)
   {
      var span = evt.Payload.Span;

      switch (evt.ChannelId)
      {
         case GameProtocol2D.ChLifecycle:
            if (GameProtocol2D.TryReadWelcome(span, out var pid, out var col, out var sx, out var sy))
            {
               _netcode.HandleWelcome(pid, col, sx, sy);
            }
            else if (GameProtocol2D.TryReadEventLog(span, out var text))
            {
               _orderedEventsReceivedCount++;
               AddFeed(text);
            }
            break;

         case GameProtocol2D.ChScoreboard:
            if (GameProtocol2D.TryReadScoreboard(span, _scoreboard))
            {
               _scoreboardsReceivedCount++;
               UpdateNameMap();
            }
            break;

         case GameProtocol2D.ChRealtime:
            if (GameProtocol2D.TryReadWorldSnapshot(span, out var tick, out var ackSeq, _snapScratchPlayers, _snapScratchOrbs, _snapScratchLasers))
            {
               _snapshotsReceivedCount++;
               _netcode.ApplyAuthoritativeSnapshot(tick, ackSeq, _snapScratchPlayers, _snapScratchOrbs, _snapScratchLasers, nowSec);
            }
            break;

         case GameProtocol2D.ChFx:
            if (GameProtocol2D.TryReadFxCue(span, out var fx))
            {
               _fxReceivedCount++;
               if (!_netcode.ShouldSuppressServerFx(fx, nowSec))
               {
                  SpawnFxVisuals(fx);
               }
            }
            break;
      }
   }

   private void SendEmote()
   {
      if (_datagramHost is null) return;

      var idx = (byte)(_emoteCursor++ % GameProtocol2D.Emotes.Length);

      if (_isHost && _hostLocalPlayer is not null)
      {
         BroadcastEvent($"{_hostLocalPlayer.Name}: \"{GameProtocol2D.Emotes[idx]}\"");
      }
      else if (_serverPeer is not null && _serverPeer.State == DatagramPeerState.Connected)
      {
         var len = GameProtocol2D.WriteEmote(_packetBuf, idx);
         _serverPeer.TrySend(GameProtocol2D.ChLifecycle, _packetBuf.AsSpan(0, len));
      }
   }

   private void CycleBots()
   {
      if (_sim is null) return;

      _botCount = (_botCount + 1) % 5;
      _sim.SetBotCount(_botCount);
   }

   private void SpawnSecondClientWindow()
   {
      var exePath = Environment.ProcessPath;
      if (string.IsNullOrEmpty(exePath)) return;

      var port = _datagramHost?.LocalEndPoint?.Port ?? _targetEndPoint.Port;
      var clientName = $"Client{Random.Shared.Next(10, 99)}";

      Process.Start(new ProcessStartInfo
      {
         FileName = exePath,
         Arguments = $"--join 127.0.0.1:{port} --name {clientName}",
         UseShellExecute = false
      });
   }

   private void BroadcastEvent(string message)
   {
      AddFeed(message);
      if (_datagramHost is null) return;

      var len = GameProtocol2D.WriteEventLog(_packetBuf, message);
      _datagramHost.Broadcast(GameProtocol2D.ChLifecycle, _packetBuf.AsSpan(0, len));
   }

   private void AddFeed(string text)
   {
      _eventFeed.Enqueue(text);
      while (_eventFeed.Count > 16) _eventFeed.Dequeue();
   }

   private void SpawnFxVisuals(FxCue2D fx, byte orbValue = 1)
   {
      var col = fx.Kind == FxKind2D.PlayerTag
         ? UiTheme.StatusBad
         : UiTheme.PlayerPalette[fx.Color % UiTheme.PlayerPalette.Length];

      var maxAge = fx.Kind switch
      {
         FxKind2D.PlayerTag => 0.40f,
         FxKind2D.OrbPickup => 0.30f,
         FxKind2D.HitSpark => 0.15f,
         _ => 0.14f
      };

      _rings.Add(new VisualRing(fx.X, fx.Y, fx.Kind, col, 0f, maxAge));

      if (fx.Kind == FxKind2D.PlayerTag)
      {
         _popups.Add(new FloatingPopup(fx.X, fx.Y - 30f, "Tag +2", UiTheme.StatusBad, 0f, 0.80f));
      }
      else if (fx.Kind == FxKind2D.OrbPickup)
      {
         var label = orbValue > 1 ? $"+{orbValue}" : "+1";
         _popups.Add(new FloatingPopup(fx.X, fx.Y - 24f, label, UiTheme.TokenGold, 0f, 0.60f));
      }
   }

   private void UpdateNameMap()
   {
      for (var i = 0; i < _scoreboard.Count; i++)
      {
         _namesById[_scoreboard[i].Id] = _scoreboard[i].Name;
      }
   }
}

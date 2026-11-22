using System.Net;
using System.Text;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Datagram.Example.ArenaGame;

public static class Program
{
   public static async Task Main(string[] args)
   {
      if (args.Length > 0)
      {
         switch (args[0].ToLowerInvariant())
         {
            case "host":
               var hostPort = args.Length > 1 && int.TryParse(args[1], out var hp) ? hp : GameProtocol.DefaultPort;
               var hostName = ReadFlag(args, "--name") ?? "Host";
               var botCount = int.TryParse(ReadFlag(args, "--bots"), out var bc) ? Math.Clamp(bc, 0, 4) : 2;
               await RunHostAsync(hostPort, hostName, botCount);
               return;

            case "join":
               var target = args.Length > 1 && !args[1].StartsWith("--") ? args[1] : $"127.0.0.1:{GameProtocol.DefaultPort}";
               var joinName = ReadFlag(args, "--name") ?? $"Pilot{Random.Shared.Next(10, 99)}";
               await RunClientAsync(ParseEndPoint(target), joinName);
               return;

            case "demo":
               var seconds = int.TryParse(ReadFlag(args, "--duration"), out var sec) ? Math.Clamp(sec, 2, 30) : 5;
               await RunHeadlessDemoAsync(seconds);
               return;
         }
      }

      Console.WriteLine("=== BESKAR ARENA (Beskar.Networking.Datagram Example) ===");
      Console.WriteLine("  1. Host Game (Listen Server - play locally + accept UDP players)");
      Console.WriteLine("  2. Join Game (Connect to a host over UDP with AEAD encryption)");
      Console.WriteLine("  3. Quick Automated Demo (1 Host + 2 Real UDP Clients for 5s)");
      Console.Write("Select mode [1]: ");
      var choice = Console.ReadLine()?.Trim();

      if (choice == "2")
      {
         Console.Write($"Host IP:Port [127.0.0.1:{GameProtocol.DefaultPort}]: ");
         var addr = Console.ReadLine()?.Trim();
         if (string.IsNullOrWhiteSpace(addr)) addr = $"127.0.0.1:{GameProtocol.DefaultPort}";

         Console.Write("Your player name [Pilot]: ");
         var name = Console.ReadLine()?.Trim();
         if (string.IsNullOrWhiteSpace(name)) name = "Pilot";

         await RunClientAsync(ParseEndPoint(addr), name);
      }
      else if (choice == "3")
      {
         await RunHeadlessDemoAsync(5);
      }
      else
      {
         Console.Write($"UDP Port [{GameProtocol.DefaultPort}]: ");
         var portStr = Console.ReadLine()?.Trim();
         var port = int.TryParse(portStr, out var p) ? p : GameProtocol.DefaultPort;

         Console.Write("Your player name [Host]: ");
         var name = Console.ReadLine()?.Trim();
         if (string.IsNullOrWhiteSpace(name)) name = "Host";

         await RunHostAsync(port, name, initialBots: 2);
      }
   }

   /// <summary>
   /// Runs an authoritative Listen Server (Host plays locally while remote clients connect over UDP).
   /// Demonstrates the fixed 30Hz game loop driving DatagramHost.Tick() and PollEvents().
   /// </summary>
   private static async Task RunHostAsync(int port, string hostName, int initialBots)
   {
      var sim = new ArenaSimulation();
      var localPlayer = sim.AddPlayer(hostName, peerId: null);
      var botCount = initialBots;
      sim.SetBotCount(botCount);

      var renderer = new ArenaRenderer();
      renderer.AddFeedLine($"Hosting on UDP port {port}. Other terminals can run: join 127.0.0.1:{port}");

      await using var host = new DatagramHost(new IPEndPoint(IPAddress.Any, port), GameProtocol.CreateOptions());

      host.Events.ValidateConnection = ctx =>
      {
         var name = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span).Trim();
         if (string.IsNullOrWhiteSpace(name)) name = $"Pilot{Random.Shared.Next(10, 99)}";
         ctx.PeerTag = name.Length > 12 ? name[..12] : name;
         return ValueTask.CompletedTask;
      };

      host.Start();

      var playersSnap = new List<PlayerSnapshot>(16);
      var orbsSnap = new List<OrbSnapshot>(16);
      var lasersSnap = new List<LaserSnapshot>(32);
      var scoreboard = new List<ScoreEntry>(16);
      var packetBuf = new byte[1024];

      var emoteCursor = 0;
      var running = true;
      ConsoleCancelEventHandler onCancel = (_, e) =>
      {
         e.Cancel = true;
         running = false;
      };
      Console.CancelKeyPress += onCancel;
      using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1000.0 / GameProtocol.TickRateHz));

      renderer.BeginConsoleSession();
      try
      {
         while (running && await timer.WaitForNextTickAsync())
         {
            // 1. Pump transport & drain incoming network events directly on the game loop thread
            host.Tick();
            host.PollEvents(evt =>
            {
               switch (evt.Type)
               {
                  case DatagramEventType.PeerConnected:
                  {
                     var playerName = evt.Peer.Tag as string ?? $"Peer{evt.Peer.PeerId}";
                     var entity = sim.AddPlayer(playerName, evt.Peer.PeerId);

                     var wLen = GameProtocol.WriteWelcome(packetBuf, entity.Id, entity.Color);
                     evt.Peer.TrySend(GameProtocol.ChLifecycle, packetBuf.AsSpan(0, wLen));
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

                     var span = evt.Payload.Span;
                     if (evt.ChannelId == GameProtocol.ChRealtime
                         && GameProtocol.TryReadClientInput(span, out var dx, out var dy, out var facing, out var fire))
                     {
                        sim.QueueInput(player, dx, dy, facing, fire);
                     }
                     else if (evt.ChannelId == GameProtocol.ChLifecycle
                              && GameProtocol.TryReadEmote(span, out var emoteIdx))
                     {
                        var text = GameProtocol.Emotes[emoteIdx % GameProtocol.Emotes.Length];
                        var msg = $"{player.Name}: \"{text}\"";
                        BroadcastEventLog(host, renderer, packetBuf, msg);
                     }
                     break;
                  }
               }
            });

            // 2. Read local Host player keyboard input
            ReadLocalControls(
               localPlayer.Facing,
               out var localDx,
               out var localDy,
               out var localFacing,
               out var localFire,
               out var sendEmote,
               out var cycleBots,
               out var quit);

            if (quit)
            {
               running = false;
               break;
            }

            if (cycleBots)
            {
               botCount = (botCount + 1) % 5;
               sim.SetBotCount(botCount);
            }

            if (sendEmote)
            {
               var text = GameProtocol.Emotes[emoteCursor++ % GameProtocol.Emotes.Length];
               BroadcastEventLog(host, renderer, packetBuf, $"{localPlayer.Name}: \"{text}\"");
            }

            sim.QueueInput(localPlayer, localDx, localDy, localFacing, localFire);

            // 3. Step authoritative simulation
            sim.Step();

            // 4. Broadcast Channel 0 (ReliableOrdered) lifecycle/tag events
            foreach (var evText in sim.PendingEvents)
            {
               BroadcastEventLog(host, renderer, packetBuf, evText);
            }
            sim.PendingEvents.Clear();

            // 5. Broadcast Channel 3 (UnreliableUnordered) fire-and-forget FX cues
            foreach (var fx in sim.PendingFx)
            {
               renderer.AddFx(fx);
               var fxLen = GameProtocol.WriteFxCue(packetBuf, fx.X, fx.Y, fx.Kind, fx.Color);
               host.Broadcast(GameProtocol.ChFx, packetBuf.AsSpan(0, fxLen));
            }
            sim.PendingFx.Clear();

            // 6. Broadcast Channel 1 (ReliableSequenced) scoreboard whenever scores/roster changed
            sim.FillScoreboard(scoreboard);
            if (sim.ScoreboardDirty)
            {
               sim.ScoreboardDirty = false;
               var sbLen = GameProtocol.WriteScoreboard(packetBuf, scoreboard);
               host.Broadcast(GameProtocol.ChScoreboard, packetBuf.AsSpan(0, sbLen));
            }

            // 7. Broadcast Channel 2 (UnreliableSequenced) 30Hz world snapshot
            sim.FillSnapshotLists(playersSnap, orbsSnap, lasersSnap);
            var snapLen = GameProtocol.WriteWorldSnapshot(packetBuf, sim.Tick, playersSnap, orbsSnap, lasersSnap);
            host.Broadcast(GameProtocol.ChRealtime, packetBuf.AsSpan(0, snapLen));

            // 8. Flush outbound datagrams immediately on the wire
            host.Tick();

            // 9. Aggregate peer stats & render frame
            var hud = BuildHostHud(host, sim.Tick);
            renderer.RenderFrame(localPlayer.Id, playersSnap, orbsSnap, lasersSnap, scoreboard, in hud);
         }

         // Graceful shutdown notify to connected clients
         foreach (var peer in host.Peers)
         {
            peer.Disconnect("Host closed arena"u8);
         }
         host.Tick();
         await Task.Delay(50);
      }
      finally
      {
         Console.CancelKeyPress -= onCancel;
         renderer.EndConsoleSession();
      }
   }

   /// <summary>
   /// Connects to a remote Listen Server over UDP with ECDH+AEAD+PSK, sends 30Hz inputs on Ch 2,
   /// and renders authoritative snapshots/scoreboards/events/FX from the host.
   /// </summary>
   private static async Task RunClientAsync(IPEndPoint serverEp, string playerName)
   {
      await using var client = new DatagramHost(GameProtocol.CreateOptions());
      client.Start();

      Console.WriteLine($"Connecting to {serverEp} as '{playerName}' (ECDH P-256 + AEAD + PSK)...");

      DatagramPeer serverPeer;
      try
      {
         serverPeer = await ConnectWithManualTickAsync(client, serverEp, Encoding.UTF8.GetBytes(playerName));
      }
      catch (Exception ex)
      {
         Console.WriteLine($"Could not connect to {serverEp}: {ex.Message}");
         return;
      }

      var renderer = new ArenaRenderer();
      renderer.AddFeedLine($"Connected to {serverEp}!");

      byte myPlayerId = 0;
      var myFacing = Direction.Right;
      uint latestTick = 0;
      var playersSnap = new List<PlayerSnapshot>(16);
      var orbsSnap = new List<OrbSnapshot>(16);
      var lasersSnap = new List<LaserSnapshot>(32);
      var scoreboard = new List<ScoreEntry>(16);
      var sendBuf = new byte[64];

      var emoteCursor = 0;
      var running = true;
      var disconnectMessage = "Left arena.";
      ConsoleCancelEventHandler onCancel = (_, e) =>
      {
         e.Cancel = true;
         running = false;
      };
      Console.CancelKeyPress += onCancel;
      using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1000.0 / GameProtocol.TickRateHz));

      renderer.BeginConsoleSession();
      try
      {
         while (running && await timer.WaitForNextTickAsync())
         {
            ReadLocalControls(
               myFacing,
               out var dx,
               out var dy,
               out myFacing,
               out var fire,
               out var sendEmote,
               out _,
               out var quit);

            if (quit)
            {
               serverPeer.Disconnect("quit"u8);
               client.Tick();
               await Task.Delay(50);
               break;
            }

            if (dx != 0 || dy != 0 || fire)
            {
               var inLen = GameProtocol.WriteClientInput(sendBuf, dx, dy, myFacing, fire);
               serverPeer.TrySend(GameProtocol.ChRealtime, sendBuf.AsSpan(0, inLen));
            }

            if (sendEmote)
            {
               var emLen = GameProtocol.WriteEmote(sendBuf, (byte)(emoteCursor++ % GameProtocol.Emotes.Length));
               serverPeer.TrySend(GameProtocol.ChLifecycle, sendBuf.AsSpan(0, emLen));
            }

            client.Tick();
            client.PollEvents(evt =>
            {
               switch (evt.Type)
               {
                  case DatagramEventType.PeerDisconnected:
                     var reasonText = evt.Payload.Length > 0
                        ? Encoding.UTF8.GetString(evt.Payload.Span)
                        : evt.DisconnectReason.ToString();
                     disconnectMessage = $"Disconnected from host: {reasonText}";
                     running = false;
                     break;

                  case DatagramEventType.MessageReceived:
                  {
                     var span = evt.Payload.Span;
                     switch (evt.ChannelId)
                     {
                        case GameProtocol.ChLifecycle:
                           if (GameProtocol.TryReadWelcome(span, out var pid, out _))
                           {
                              myPlayerId = pid;
                           }
                           else if (GameProtocol.TryReadEventLog(span, out var logLine))
                           {
                              renderer.AddFeedLine(logLine);
                           }
                           break;

                        case GameProtocol.ChScoreboard:
                           GameProtocol.TryReadScoreboard(span, scoreboard);
                           break;

                        case GameProtocol.ChRealtime:
                           GameProtocol.TryReadWorldSnapshot(span, out latestTick, playersSnap, orbsSnap, lasersSnap);
                           break;

                        case GameProtocol.ChFx:
                           if (GameProtocol.TryReadFxCue(span, out var fx))
                           {
                              renderer.AddFx(fx);
                           }
                           break;
                     }
                     break;
                  }
               }
            });

            var st = serverPeer.Stats;
            var hud = new NetworkHudInfo(
               RoleLabel: $"CLIENT ({playerName})",
               EndPointLabel: serverEp.ToString(),
               RemotePeers: client.PeerCount,
               RttMs: st.SmoothedRttMs,
               LossPercent: st.PacketLossRate * 100.0,
               CongestionLabel: st.CongestionState.ToString(),
               BytesSent: st.BytesSent,
               BytesReceived: st.BytesReceived,
               ServerTick: latestTick,
               ShowBotHint: false);

            renderer.RenderFrame(myPlayerId, playersSnap, orbsSnap, lasersSnap, scoreboard, in hud);
         }
         if (serverPeer.State == DatagramPeerState.Connected)
         {
            serverPeer.Disconnect("quit"u8);
            client.Tick();
            await Task.Delay(50);
         }
      }
      finally
      {
         Console.CancelKeyPress -= onCancel;
         renderer.EndConsoleSession();
         Console.WriteLine(disconnectMessage);
      }
   }

   /// <summary>
   /// Runs a self-contained real-UDP demo (1 Host + 2 autonomous UDP Clients over loopback)
   /// for verification or quick showcasing without needing multiple terminals.
   /// </summary>
   private static async Task RunHeadlessDemoAsync(int durationSeconds)
   {
      Console.WriteLine($"Starting {durationSeconds}s live real-UDP Arena Demo (1 Host + 2 AEAD UDP Clients + 1 Bot)...");

      var sim = new ArenaSimulation();
      sim.SetBotCount(1);

      await using var host = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), GameProtocol.CreateOptions());
      host.Events.ValidateConnection = ctx =>
      {
         ctx.PeerTag = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span);
         return ValueTask.CompletedTask;
      };
      host.Start();
      var serverEp = host.LocalEndPoint!;

      await using var clientA = new DatagramHost(GameProtocol.CreateOptions());
      await using var clientB = new DatagramHost(GameProtocol.CreateOptions());
      clientA.Start();
      clientB.Start();

      var connectA = clientA.ConnectAsync(serverEp, "Alice"u8.ToArray());
      var connectB = clientB.ConnectAsync(serverEp, "Bob"u8.ToArray());

      while (!connectA.IsCompleted || !connectB.IsCompleted)
      {
         host.Tick();
         clientA.Tick();
         clientB.Tick();
         await Task.Delay(10);
      }

      var peerA = await connectA;
      var peerB = await connectB;

      var snapsA = 0;
      var snapsB = 0;
      var scoreUpdatesA = 0;
      var fxA = 0;
      var eventsA = new List<string>();
      var latestScores = new List<ScoreEntry>();
      var tmpPlayers = new List<PlayerSnapshot>();
      var tmpOrbs = new List<OrbSnapshot>();
      var tmpLasers = new List<LaserSnapshot>();
      var buf = new byte[1024];

      var totalTicks = durationSeconds * GameProtocol.TickRateHz;
      using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1000.0 / GameProtocol.TickRateHz));

      for (var t = 0; t < totalTicks && await timer.WaitForNextTickAsync(); t++)
      {
         // Autonomous client inputs sent on Ch 2 (UnreliableSequenced)
         var dirA = (Direction)(t / 8 % 4);
         var (dxA, dyA) = DirDelta(dirA);
         var lenA = GameProtocol.WriteClientInput(buf, dxA, dyA, dirA, fire: t % 12 == 0);
         peerA.TrySend(GameProtocol.ChRealtime, buf.AsSpan(0, lenA));

         var dirB = (Direction)((t / 6 + 2) % 4);
         var (dxB, dyB) = DirDelta(dirB);
         var lenB = GameProtocol.WriteClientInput(buf, dxB, dyB, dirB, fire: t % 10 == 0);
         peerB.TrySend(GameProtocol.ChRealtime, buf.AsSpan(0, lenB));

         if (t == 30)
         {
            var eLen = GameProtocol.WriteEmote(buf, 0);
            peerA.TrySend(GameProtocol.ChLifecycle, buf.AsSpan(0, eLen));
         }

         clientA.Tick();
         clientB.Tick();
         host.Tick();

         host.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.PeerConnected)
            {
               var p = sim.AddPlayer(evt.Peer.Tag as string ?? "Peer", evt.Peer.PeerId);
               var wLen = GameProtocol.WriteWelcome(buf, p.Id, p.Color);
               evt.Peer.TrySend(GameProtocol.ChLifecycle, buf.AsSpan(0, wLen));
            }
            else if (evt.Type == DatagramEventType.MessageReceived)
            {
               var p = sim.FindByPeer(evt.Peer.PeerId);
               if (p is null) return;
               if (evt.ChannelId == GameProtocol.ChRealtime
                   && GameProtocol.TryReadClientInput(evt.Payload.Span, out var dx, out var dy, out var f, out var fire))
               {
                  sim.QueueInput(p, dx, dy, f, fire);
               }
               else if (evt.ChannelId == GameProtocol.ChLifecycle
                        && GameProtocol.TryReadEmote(evt.Payload.Span, out var idx))
               {
                  var eLen = GameProtocol.WriteEventLog(buf, $"{p.Name}: \"{GameProtocol.Emotes[idx % GameProtocol.Emotes.Length]}\"");
                  host.Broadcast(GameProtocol.ChLifecycle, buf.AsSpan(0, eLen));
               }
            }
         });

         sim.Step();

         foreach (var ev in sim.PendingEvents)
         {
            var eLen = GameProtocol.WriteEventLog(buf, ev);
            host.Broadcast(GameProtocol.ChLifecycle, buf.AsSpan(0, eLen));
         }
         sim.PendingEvents.Clear();

         foreach (var fx in sim.PendingFx)
         {
            var fxLen = GameProtocol.WriteFxCue(buf, fx.X, fx.Y, fx.Kind, fx.Color);
            host.Broadcast(GameProtocol.ChFx, buf.AsSpan(0, fxLen));
         }
         sim.PendingFx.Clear();

         if (sim.ScoreboardDirty)
         {
            sim.ScoreboardDirty = false;
            sim.FillScoreboard(latestScores);
            var sbLen = GameProtocol.WriteScoreboard(buf, latestScores);
            host.Broadcast(GameProtocol.ChScoreboard, buf.AsSpan(0, sbLen));
         }

         sim.FillSnapshotLists(tmpPlayers, tmpOrbs, tmpLasers);
         var sLen = GameProtocol.WriteWorldSnapshot(buf, sim.Tick, tmpPlayers, tmpOrbs, tmpLasers);
         host.Broadcast(GameProtocol.ChRealtime, buf.AsSpan(0, sLen));
         host.Tick();

         clientA.PollEvents(evt =>
         {
            if (evt.Type != DatagramEventType.MessageReceived) return;
            if (evt.ChannelId == GameProtocol.ChRealtime) snapsA++;
            else if (evt.ChannelId == GameProtocol.ChScoreboard) scoreUpdatesA++;
            else if (evt.ChannelId == GameProtocol.ChFx) fxA++;
            else if (evt.ChannelId == GameProtocol.ChLifecycle && GameProtocol.TryReadEventLog(evt.Payload.Span, out var txt))
            {
               eventsA.Add(txt);
            }
         });

         clientB.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == GameProtocol.ChRealtime) snapsB++;
         });
      }

      sim.FillScoreboard(latestScores);
      var stA = peerA.Stats;
      var stB = peerB.Stats;

      Console.WriteLine($"Completed {sim.Tick} server ticks on real UDP ({serverEp})!");
      Console.WriteLine($"  Alice -> Snapshots(Ch2): {snapsA} | Scoreboards(Ch1): {scoreUpdatesA} | FX(Ch3): {fxA} | RTT: {stA.SmoothedRttMs:F1}ms | RX: {stA.BytesReceived / 1024.0:F1} KB");
      Console.WriteLine($"  Bob   -> Snapshots(Ch2): {snapsB} | RTT: {stB.SmoothedRttMs:F1}ms | RX: {stB.BytesReceived / 1024.0:F1} KB");
      Console.WriteLine("  Final Scoreboard:");
      foreach (var entry in latestScores)
      {
         Console.WriteLine($"    - {entry.Name,-12} : {entry.Score} pts");
      }
      Console.WriteLine($"  Ordered Events Received by Alice (Ch0): {eventsA.Count}");
      foreach (var ev in eventsA.TakeLast(5))
      {
         Console.WriteLine($"    > {ev}");
      }

      peerA.Disconnect("demo done"u8);
      peerB.Disconnect("demo done"u8);
      clientA.Tick();
      clientB.Tick();
      await Task.Delay(30);
      host.Tick();
   }

   private static async Task<DatagramPeer> ConnectWithManualTickAsync(DatagramHost client, IPEndPoint serverEp, byte[] metadata)
   {
      var connectTask = client.ConnectAsync(serverEp, metadata);
      while (!connectTask.IsCompleted)
      {
         client.Tick();
         await Task.Delay(15);
      }

      return await connectTask;
   }

   private static void BroadcastEventLog(DatagramHost host, ArenaRenderer renderer, byte[] buf, string message)
   {
      renderer.AddFeedLine(message);
      var len = GameProtocol.WriteEventLog(buf, message);
      host.Broadcast(GameProtocol.ChLifecycle, buf.AsSpan(0, len));
   }

   private static NetworkHudInfo BuildHostHud(DatagramHost host, uint tick)
   {
      var peers = host.Peers;
      double rttSum = 0;
      double lossSum = 0;
      long bytesSent = 0;
      long bytesRecv = 0;
      var count = 0;
      var worst = DatagramCongestionState.Healthy;

      foreach (var p in peers)
      {
         var st = p.Stats;
         count++;
         rttSum += st.SmoothedRttMs;
         lossSum += st.PacketLossRate;
         bytesSent += st.BytesSent;
         bytesRecv += st.BytesReceived;
         if (st.CongestionState > worst) worst = st.CongestionState;
      }

      return new NetworkHudInfo(
         RoleLabel: "HOST (Listen Server)",
         EndPointLabel: host.LocalEndPoint?.ToString() ?? "?",
         RemotePeers: count,
         RttMs: count > 0 ? rttSum / count : 0.0,
         LossPercent: count > 0 ? (lossSum / count) * 100.0 : 0.0,
         CongestionLabel: count > 0 ? worst.ToString() : "Local",
         BytesSent: bytesSent,
         BytesReceived: bytesRecv,
         ServerTick: tick,
         ShowBotHint: true);
   }

   private static void ReadLocalControls(
      Direction currentFacing,
      out sbyte dx,
      out sbyte dy,
      out Direction facing,
      out bool fire,
      out bool sendEmote,
      out bool cycleBots,
      out bool quit)
   {
      dx = 0;
      dy = 0;
      facing = currentFacing;
      fire = false;
      sendEmote = false;
      cycleBots = false;
      quit = false;

      if (Console.IsInputRedirected) return;

      while (Console.KeyAvailable)
      {
         var key = Console.ReadKey(intercept: true).Key;
         switch (key)
         {
            case ConsoleKey.W or ConsoleKey.UpArrow:
               dx = 0; dy = -1; facing = Direction.Up;
               break;
            case ConsoleKey.S or ConsoleKey.DownArrow:
               dx = 0; dy = 1; facing = Direction.Down;
               break;
            case ConsoleKey.A or ConsoleKey.LeftArrow:
               dx = -1; dy = 0; facing = Direction.Left;
               break;
            case ConsoleKey.D or ConsoleKey.RightArrow:
               dx = 1; dy = 0; facing = Direction.Right;
               break;
            case ConsoleKey.Spacebar or ConsoleKey.F:
               fire = true;
               break;
            case ConsoleKey.T:
               sendEmote = true;
               break;
            case ConsoleKey.B:
               cycleBots = true;
               break;
            case ConsoleKey.Q or ConsoleKey.Escape:
               quit = true;
               break;
         }
      }
   }

   private static (sbyte Dx, sbyte Dy) DirDelta(Direction dir) => dir switch
   {
      Direction.Up => (0, -1),
      Direction.Down => (0, 1),
      Direction.Left => (-1, 0),
      _ => (1, 0)
   };

   private static IPEndPoint ParseEndPoint(string raw)
   {
      var parts = raw.Split(':', 2);
      var ip = IPAddress.Parse(parts[0]);
      var port = parts.Length == 2 && int.TryParse(parts[1], out var p) ? p : GameProtocol.DefaultPort;
      return new IPEndPoint(ip, port);
   }

   private static string? ReadFlag(string[] args, string flag)
   {
      for (var i = 0; i < args.Length - 1; i++)
      {
         if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase))
         {
            return args[i + 1];
         }
      }
      return null;
   }
}

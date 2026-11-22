using System.Net;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;
using Beskar.Datagram.Example.ArenaGame2D.UI;

namespace Beskar.Datagram.Example.ArenaGame2D.Testing;

/// <summary>
/// Automated real-UDP visual & netcode regression harness (<c>--self-test &lt;outDir&gt;</c>).
/// Drives real Host + Client windows across LAN, smoothness, heavy lag, rapid-fire prediction,
/// server-side hitbox rewind, reconciliation flash, prediction OFF, and extreme packet loss.
/// </summary>
internal static class ArenaSelfTestRunner
{
   public static int Run(string outDir)
   {
      Directory.CreateDirectory(outDir);
      var exitCode = 0;

      // 1. Capture Launcher screenshot
      using (var launcher = new LauncherDialog())
      {
         launcher.CreateControl();
         launcher.CaptureToFile(Path.Combine(outDir, "01_launcher.png"));
      }

      // 2. Drive real Host + Client windows inside a WinForms ApplicationContext
      var context = new ApplicationContext();
      _ = RunScenarioAsync(outDir, context, code => exitCode = code);
      Application.Run(context);

      return exitCode;
   }

   private static async Task RunScenarioAsync(string outDir, ApplicationContext context, Action<int> setExitCode)
   {
      var failures = new List<string>();
      var reportLines = new List<string>();

      GameCanvasForm? hostForm = null;
      GameCanvasForm? clientForm = null;

      try
      {
         hostForm = new GameCanvasForm(isHost: true, new IPEndPoint(IPAddress.Loopback, 0), "Alice", initialBots: 2, showOnScreen: false);
         hostForm.Show();
         await hostForm.ReadyTask;

         var hostPort = hostForm.BoundEndPoint!.Port;
         reportLines.Add($"[SETUP] Host bound on 127.0.0.1:{hostPort}");

         clientForm = new GameCanvasForm(isHost: false, new IPEndPoint(IPAddress.Loopback, hostPort), "Client1", initialBots: 0, showOnScreen: false);
         clientForm.Show();
         await clientForm.ReadyTask;

         if (!clientForm.IsClientConnected)
         {
            failures.Add("Client failed to connect to Host over real UDP.");
            return;
         }

         await RunStage1LanAsync(hostForm, clientForm, outDir, reportLines, failures);
         await RunStage1BSmoothnessAsync(hostForm, clientForm, reportLines, failures);
         await RunStage2LaggyPredictionAsync(hostForm, clientForm, outDir, reportLines, failures);
         await RunStage2CRapidFireAsync(hostForm, clientForm, reportLines, failures);
         await RunStage2DLagCompRewindAsync(hostForm, clientForm, outDir, reportLines, failures);
         await RunStage3ReconciliationAsync(hostForm, clientForm, outDir, reportLines, failures);
         await RunStage4PredictionOffAsync(clientForm, outDir, reportLines, failures);
         await RunStage5ExtremeHostLagAsync(hostForm, clientForm, outDir, reportLines, failures);
      }
      catch (Exception ex)
      {
         failures.Add($"Unhandled exception in self-test: {ex}");
      }
      finally
      {
         WriteReport(outDir, reportLines, failures, setExitCode);

         if (clientForm is not null && !clientForm.IsDisposed) clientForm.Close();
         if (hostForm is not null && !hostForm.IsDisposed) hostForm.Close();

         await Task.Delay(80);
         context.ExitThread();
      }
   }

   private static async Task RunStage1LanAsync(
      GameCanvasForm hostForm,
      GameCanvasForm clientForm,
      string outDir,
      List<string> reportLines,
      List<string> failures)
   {
      var hostStep = 0;
      hostForm.SetScriptedInput(() =>
      {
         hostStep++;
         var p = (hostStep / 50) % 4;
         var b = p switch
         {
            0 => InputButtons.Right | InputButtons.Down,
            1 => InputButtons.Left,
            2 => InputButtons.Up,
            _ => InputButtons.Right
         };
         if (hostStep % 22 == 0) b |= InputButtons.Fire;
         return (b, p * 1.4f);
      });

      var clientStep = 0;
      clientForm.SetScriptedInput(() =>
      {
         clientStep++;
         var p = (clientStep / 55) % 4;
         var b = p switch
         {
            0 => InputButtons.Right,
            1 => InputButtons.Down,
            2 => InputButtons.Left,
            _ => InputButtons.Up
         };
         if (clientStep % 18 == 0) b |= InputButtons.Fire;
         return (b, p * MathF.PI * 0.5f);
      });

      clientForm.SetDebugOverlay(false);
      hostForm.SetDebugOverlay(true);

      await Task.Delay(700);
      clientForm.TriggerEmote();
      hostForm.TriggerEmote();
      await Task.Delay(900);

      hostForm.CaptureScreenshotToFile(Path.Combine(outDir, "02_host_lan_debug.png"));
      clientForm.CaptureScreenshotToFile(Path.Combine(outDir, "03_client_lan_clean.png"));

      var lanRtt = clientForm.ClientServerPeer?.Stats.SmoothedRttMs ?? -1;
      reportLines.Add($"[STAGE 1: LAN] RTT={lanRtt:F1}ms | Snaps={clientForm.SnapshotsReceivedCount} | Scoreboards={clientForm.ScoreboardsReceivedCount} | FX={clientForm.FxReceivedCount} | Events={clientForm.OrderedEventsReceivedCount}");

      if (clientForm.SnapshotsReceivedCount < 15) failures.Add($"Too few LAN snapshots received ({clientForm.SnapshotsReceivedCount}).");
      if (clientForm.ScoreboardsReceivedCount < 1) failures.Add("Client received 0 scoreboard updates on Ch 1.");
      if (clientForm.OrderedEventsReceivedCount < 2) failures.Add($"Client received too few Ch 0 ordered events ({clientForm.OrderedEventsReceivedCount}).");
      if (clientForm.FxReceivedCount < 1) failures.Add("Client received 0 Ch 3 FX cues.");
   }

   private static async Task RunStage1BSmoothnessAsync(
      GameCanvasForm hostForm,
      GameCanvasForm clientForm,
      List<string> reportLines,
      List<string> failures)
   {
      hostForm.HostSimulation!.SetBotCount(0);

      var hostEntity = hostForm.HostSimulation.Players.First(p => !p.PeerId.HasValue && !p.IsBot);
      var clientEntity = hostForm.HostSimulation.Players.First(p => p.PeerId.HasValue);

      hostEntity.X = 80f; hostEntity.Y = 85f; hostEntity.StunTicks = 0;
      clientEntity.X = 80f; clientEntity.Y = 480f; clientEntity.StunTicks = 0;

      hostForm.SetScriptedInput(() => (InputButtons.Right, 0f));
      clientForm.SetScriptedInput(() => (InputButtons.Right, 0f));
      await Task.Delay(250);

      var clientWatchingHostSpeeds = new List<double>(128);
      var hostWatchingClientSpeeds = new List<double>(128);
      var clientWatchFrozen = 0;
      var hostWatchFrozen = 0;

      var lastClientSampleT = 0.0;
      var lastHostSampleT = 0.0;
      var lastRemoteHostX = -1f;
      var lastRemoteClientX = -1f;

      var hostPlayerId = hostEntity.Id;
      var clientPlayerId = clientEntity.Id;

      Action<double, IReadOnlyList<PlayerState2D>> onClientFrame = (tSec, list) =>
      {
         for (var i = 0; i < list.Count; i++)
         {
            if (list[i].Id != hostPlayerId) continue;

            if (lastClientSampleT > 0 && tSec - lastClientSampleT > 0.003)
            {
               var dx = list[i].X - lastRemoteHostX;
               if (MathF.Abs(dx) < 0.15f) clientWatchFrozen++;
               clientWatchingHostSpeeds.Add(dx / (tSec - lastClientSampleT));
            }

            lastClientSampleT = tSec;
            lastRemoteHostX = list[i].X;
            break;
         }
      };

      Action<double, IReadOnlyList<PlayerState2D>> onHostFrame = (tSec, list) =>
      {
         for (var i = 0; i < list.Count; i++)
         {
            if (list[i].Id != clientPlayerId) continue;

            if (lastHostSampleT > 0 && tSec - lastHostSampleT > 0.003)
            {
               var dx = list[i].X - lastRemoteClientX;
               if (MathF.Abs(dx) < 0.15f) hostWatchFrozen++;
               hostWatchingClientSpeeds.Add(dx / (tSec - lastHostSampleT));
            }

            lastHostSampleT = tSec;
            lastRemoteClientX = list[i].X;
            break;
         }
      };

      clientForm.FrameRendered += onClientFrame;
      hostForm.FrameRendered += onHostFrame;
      await Task.Delay(1100);
      clientForm.FrameRendered -= onClientFrame;
      hostForm.FrameRendered -= onHostFrame;

      hostForm.HostSimulation.SetBotCount(2);

      var (cMean, cCv) = ComputeSpeedStats(clientWatchingHostSpeeds);
      var (hMean, hCv) = ComputeSpeedStats(hostWatchingClientSpeeds);

      reportLines.Add($"[STAGE 1B: LAN SMOOTHNESS] Client->Host: {cMean:F1}px/s (CV={cCv:F3}, frozen={clientWatchFrozen}/{clientWatchingHostSpeeds.Count}) | Host->Client: {hMean:F1}px/s (CV={hCv:F3}, frozen={hostWatchFrozen}/{hostWatchingClientSpeeds.Count})");

      if (cCv > 0.20) failures.Add($"Client viewing Host had high velocity jitter (CV={cCv:F3}, expected <= 0.20).");
      if (hCv > 0.25) failures.Add($"Host viewing Client had high velocity jitter (CV={hCv:F3}, expected <= 0.25).");
   }

   private static async Task RunStage2LaggyPredictionAsync(
      GameCanvasForm hostForm,
      GameCanvasForm clientForm,
      string outDir,
      List<string> reportLines,
      List<string> failures)
   {
      clientForm.SetDebugOverlay(true);
      clientForm.SetNetcodeOptions(prediction: true, interpolation: true, redundancy: true);
      clientForm.SetFaultProfile(delayMs: 150, jitterMs: 25, lossPct: 12);

      var remoteForSprint = hostForm.HostSimulation!.Players.FirstOrDefault(p => p.PeerId.HasValue);
      if (remoteForSprint is not null)
      {
         remoteForSprint.X = 120f;
         remoteForSprint.Y = 92f;
         remoteForSprint.StunTicks = 0;
         hostForm.HostSimulation.SpawnOrbAt(280f, 92f, value: 3);
      }

      var sprintStep = 0;
      var maxGhostLead = 0f;

      Action<double, IReadOnlyList<PlayerState2D>> onStage2Frame = (_, _) =>
      {
         var g = clientForm.Netcode.ServerGhostDistancePx;
         if (g > maxGhostLead) maxGhostLead = g;
      };

      clientForm.FrameRendered += onStage2Frame;
      clientForm.SetScriptedInput(() =>
      {
         sprintStep++;
         var b = InputButtons.Right;
         if (sprintStep % 24 == 0) b |= InputButtons.Fire;
         return (b, 0f);
      });

      await Task.Delay(1700);
      clientForm.FrameRendered -= onStage2Frame;

      clientForm.CaptureScreenshotToFile(Path.Combine(outDir, "04_client_laggy_pred_on_debug.png"));

      var lagRtt = clientForm.ClientServerPeer?.Stats.SmoothedRttMs ?? 0;
      var ghostLead = MathF.Max(clientForm.Netcode.ServerGhostDistancePx, maxGhostLead);
      var pendingInputs = clientForm.Netcode.PendingUnackedCount;
      var drops = clientForm.FaultTransport.DroppedPackets;

      reportLines.Add($"[STAGE 2: LAGGY PRED ON] RTT={lagRtt:F1}ms | SimDrops={drops} | MaxGhostLead={ghostLead:F1}px | PendingInputs={pendingInputs} | Reconciles={clientForm.Netcode.ReconciliationCorrections} | InterpDelay={clientForm.Netcode.AdaptiveInterpDelaySec * 1000:F0}ms");
      reportLines.Add($"[STAGE 2B: SHOTS & PICKUPS] PredShots={clientForm.Netcode.PredictedShotsFired} (confirmed={clientForm.Netcode.PredictedShotsConfirmed}) | OptPickups={clientForm.Netcode.OptimisticPickupsTriggered} (confirmed={clientForm.Netcode.OptimisticPickupsConfirmed}, rollbacks={clientForm.Netcode.OptimisticPickupsRolledBack})");

      if (lagRtt < 200) failures.Add($"Expected measured RTT >= 200ms under 150ms one-way delay, got {lagRtt:F1}ms.");
      if (drops < 5) failures.Add($"Expected real UDP drops under 12% loss, got {drops}.");
      if (pendingInputs < 5) failures.Add($"Expected pending unacked predicted inputs >= 5 at ~320ms RTT, got {pendingInputs}.");
      if (ghostLead < 20f) failures.Add($"Expected visible Server Ghost lead >= 20px during laggy sprint, got {ghostLead:F1}px.");
      if (clientForm.Netcode.PredictedShotsFired < 1) failures.Add("Expected client-side predicted shots >= 1.");
      if (clientForm.Netcode.PredictedShotsConfirmed < 1) failures.Add("Expected server confirmation of predicted shots >= 1.");
      if (clientForm.Netcode.OptimisticPickupsTriggered < 1) failures.Add("Expected optimistic orb pickup >= 1.");
      if (clientForm.Netcode.OptimisticPickupsConfirmed < 1) failures.Add("Expected server confirmation of optimistic orb pickup >= 1.");
   }

   private static async Task RunStage2CRapidFireAsync(
      GameCanvasForm hostForm,
      GameCanvasForm clientForm,
      List<string> reportLines,
      List<string> failures)
   {
      hostForm.HostSimulation!.SetBotCount(0);
      hostForm.SetFaultProfile(delayMs: 40, jitterMs: 8, lossPct: 1);
      clientForm.SetFaultProfile(delayMs: 40, jitterMs: 8, lossPct: 1);
      clientForm.SetNetcodeOptions(prediction: true, interpolation: true, redundancy: true, predictShotsAndPickups: true);

      var shooterEntity = hostForm.HostSimulation.Players.First(p => p.PeerId.HasValue);
      shooterEntity.X = 60f;
      shooterEntity.Y = 85f;
      shooterEntity.StunTicks = 0;

      var myShooterId = clientForm.Netcode.MyPlayerId;

      clientForm.SetScriptedInput(() => (InputButtons.Right | InputButtons.Fire, 0f));
      await Task.Delay(250);

      var bulletPrevX = new Dictionary<uint, (double T, float X)>();
      var bulletSpeeds = new List<double>(128);
      var bulletBackwardJumps = 0;

      Action<double, IReadOnlyList<PlayerState2D>> onRapidFireFrame = (tSec, _) =>
      {
         var lasers = clientForm.RenderedLasers;
         for (var i = 0; i < lasers.Count; i++)
         {
            var l = lasers[i];
            if (l.OwnerId != myShooterId || l.ShotSeq == 0) continue;

            if (bulletPrevX.TryGetValue(l.ShotSeq, out var prev) && tSec - prev.T > 0.003)
            {
               var dx = l.X - prev.X;
               if (dx < -0.5f) bulletBackwardJumps++;
               else if (dx > 0.1f) bulletSpeeds.Add(dx / (tSec - prev.T));
            }

            bulletPrevX[l.ShotSeq] = (tSec, l.X);
         }
      };

      clientForm.FrameRendered += onRapidFireFrame;
      await Task.Delay(1200);
      clientForm.FrameRendered -= onRapidFireFrame;

      var (bMean, bCv) = ComputeSpeedStats(bulletSpeeds);
      reportLines.Add($"[STAGE 2C: RAPID-FIRE 40+40ms] BulletSpeed={bMean:F1}px/s (CV={bCv:F3}, samples={bulletSpeeds.Count}, backwardJumps={bulletBackwardJumps})");

      if (bulletSpeeds.Count < 15) failures.Add($"Too few rapid-fire bullet samples collected ({bulletSpeeds.Count}).");
      if (bulletBackwardJumps > 0) failures.Add($"Shooter saw {bulletBackwardJumps} backward bullet stutter jump(s)!");
      if (bCv > 0.12) failures.Add($"Rapid-fire predicted bullets had high velocity jitter (CV={bCv:F3}, expected <= 0.12).");
   }

   private static async Task RunStage2DLagCompRewindAsync(
      GameCanvasForm hostForm,
      GameCanvasForm clientForm,
      string outDir,
      List<string> reportLines,
      List<string> failures)
   {
      hostForm.SetLagCompensation(true);
      hostForm.SetDebugOverlay(true);
      hostForm.SetFaultProfile(0, 0, 0);
      clientForm.SetFaultProfile(delayMs: 140, jitterMs: 10, lossPct: 2);
      clientForm.SetNetcodeOptions(prediction: true, interpolation: true, redundancy: true, predictShotsAndPickups: true);

      var shooterEntity = hostForm.HostSimulation!.Players.First(p => p.PeerId.HasValue);
      var hostTarget = hostForm.HostSimulation.Players.First(p => !p.PeerId.HasValue && !p.IsBot);

      hostForm.SetScriptedInput(() => (InputButtons.None, 0f));
      clientForm.SetScriptedInput(() => (InputButtons.None, MathF.PI * 0.5f));
      await Task.Delay(260);
      hostForm.HostSimulation.ClearLasers();

      shooterEntity.X = 400f;
      shooterEntity.Y = 52f;
      shooterEntity.StunTicks = 0;
      shooterEntity.FireCooldownTicks = 0;

      hostTarget.X = 220f;
      hostTarget.Y = 102f;
      hostTarget.StunTicks = 0;
      hostTarget.FireCooldownTicks = 0;

      await Task.Delay(550);
      hostForm.HostSimulation.ClearLasers();
      hostTarget.StunTicks = 0;

      var lagCompBefore = hostForm.HostSimulation.LagCompensatedHits;
      var cosmeticBefore = clientForm.Netcode.PredictedCosmeticHits;
      var sawRemoteStun = false;
      var firedLagCompShot = false;

      hostForm.SetScriptedInput(() => (InputButtons.Right, 0f));
      clientForm.SetScriptedInput(() =>
      {
         var players = clientForm.RenderedPlayers;
         for (var i = 0; i < players.Count; i++)
         {
            if (players[i].Id != hostTarget.Id) continue;

            if (players[i].Stunned) sawRemoteStun = true;

            if (!firedLagCompShot && players[i].X >= 384f && players[i].X <= 402f)
            {
               firedLagCompShot = true;
               return (InputButtons.Fire, MathF.PI * 0.5f);
            }
            break;
         }

         return (InputButtons.None, MathF.PI * 0.5f);
      });

      var capturedRewindShot = false;
      for (var w = 0; w < 90; w++)
      {
         if (!capturedRewindShot && hostForm.HostSimulation.LagCompensatedHits > lagCompBefore)
         {
            hostForm.CaptureScreenshotToFile(Path.Combine(outDir, "04b_host_lagcomp_rewind.png"));
            capturedRewindShot = true;
         }

         if (capturedRewindShot && sawRemoteStun) break;
         await Task.Delay(25);
      }

      if (!capturedRewindShot)
      {
         hostForm.CaptureScreenshotToFile(Path.Combine(outDir, "04b_host_lagcomp_rewind.png"));
      }

      var lagCompDelta = hostForm.HostSimulation.LagCompensatedHits - lagCompBefore;
      var cosmeticDelta = clientForm.Netcode.PredictedCosmeticHits - cosmeticBefore;
      var rewindMs = hostForm.HostSimulation.LastRewindMs;

      reportLines.Add($"[STAGE 2D: LAG-COMP REWIND] Fired={firedLagCompShot} | CosmeticHits={cosmeticDelta} | ServerRewindHits={lagCompDelta} (rewind=-{rewindMs:F0}ms) | RemoteStunSeen={sawRemoteStun}");

      if (!firedLagCompShot) failures.Add("Stage 2D shooter never saw moving target enter crosshair.");
      if (cosmeticDelta < 1) failures.Add("Expected local cosmetic hit spark on predicted bullet contact.");
      if (lagCompDelta < 1) failures.Add("Server-side lag compensation (hitbox rewind) did not register hit on fast-moving target under ~280ms RTT.");
      if (rewindMs < 150f) failures.Add($"Expected server hitbox rewind >= 150ms under 140ms one-way lag, got {rewindMs:F0}ms.");
      if (!sawRemoteStun) failures.Add("Client did not observe authoritative remote Stunned state after lag-compensated tag.");

      hostForm.SetScriptedInput(null);
      hostForm.HostSimulation.SetBotCount(2);
      hostForm.SetFaultProfile(0, 0, 0);
      clientForm.SetFaultProfile(150, 25, 12);
      clientForm.SetScriptedInput(() => (InputButtons.Right, 0f));
      await Task.Delay(250);
   }

   private static async Task RunStage3ReconciliationAsync(
      GameCanvasForm hostForm,
      GameCanvasForm clientForm,
      string outDir,
      List<string> reportLines,
      List<string> failures)
   {
      var reconcilesBefore = clientForm.Netcode.ReconciliationCorrections;
      var remoteEntity = hostForm.HostSimulation!.Players.FirstOrDefault(p => p.PeerId.HasValue);
      var hostEntity = hostForm.HostSimulation!.Players.FirstOrDefault(p => !p.PeerId.HasValue && !p.IsBot);

      if (remoteEntity is not null)
      {
         hostForm.HostSimulation.TriggerAuthoritativeTag(remoteEntity, hostEntity, 0.85f, -0.52f, 0);
      }

      for (var w = 0; w < 25 && clientForm.Netcode.ReconciliationCorrections == reconcilesBefore; w++)
      {
         await Task.Delay(35);
      }

      clientForm.CaptureScreenshotToFile(Path.Combine(outDir, "05_client_reconcile_flash.png"));

      var reconcilesAfter = clientForm.Netcode.ReconciliationCorrections;
      reportLines.Add($"[STAGE 3: RECONCILIATION] Corrections {reconcilesBefore} -> {reconcilesAfter} | LastErr={clientForm.Netcode.LastReconcileErrorPx:F1}px | MaxErr={clientForm.Netcode.MaxReconcileErrorPx:F1}px");

      if (reconcilesAfter <= reconcilesBefore) failures.Add("Authoritative knockback did not trigger Server Reconciliation on client.");
   }

   private static async Task RunStage4PredictionOffAsync(
      GameCanvasForm clientForm,
      string outDir,
      List<string> reportLines,
      List<string> failures)
   {
      clientForm.SetNetcodeOptions(prediction: false, interpolation: true, redundancy: true);
      await Task.Delay(900);

      clientForm.CaptureScreenshotToFile(Path.Combine(outDir, "06_client_laggy_pred_off.png"));

      reportLines.Add($"[STAGE 4: LAGGY PRED OFF] PendingInputs={clientForm.Netcode.PendingUnackedCount} | GhostLead={clientForm.Netcode.ServerGhostDistancePx:F1}px");
      if (clientForm.Netcode.PendingUnackedCount != 0) failures.Add("Prediction OFF should clear PendingUnackedCount to 0.");
   }

   private static async Task RunStage5ExtremeHostLagAsync(
      GameCanvasForm hostForm,
      GameCanvasForm clientForm,
      string outDir,
      List<string> reportLines,
      List<string> failures)
   {
      clientForm.SetFaultProfile(0, 0, 0);
      clientForm.SetNetcodeOptions(prediction: true, interpolation: true, redundancy: true);
      hostForm.SetFaultProfile(delayMs: 220, jitterMs: 40, lossPct: 18);
      await Task.Delay(1600);

      hostForm.CaptureScreenshotToFile(Path.Combine(outDir, "07_host_extreme_lag.png"));

      var extremeRtt = clientForm.ClientServerPeer?.Stats.SmoothedRttMs ?? 0;
      var hostDrops = hostForm.FaultTransport.DroppedPackets;

      reportLines.Add($"[STAGE 5: EXTREME LAG ON HOST] Connected={clientForm.IsClientConnected} | RTT={extremeRtt:F1}ms | HostSimDrops={hostDrops} | StaleSnapViolations={clientForm.Netcode.StaleSnapshotViolations}");

      if (!clientForm.IsClientConnected) failures.Add("Client disconnected under 220ms ±40ms / 18% loss.");
      if (clientForm.Netcode.StaleSnapshotViolations != 0) failures.Add($"UnreliableSequenced allowed {clientForm.Netcode.StaleSnapshotViolations} stale snapshots!");
      if (clientForm.CurrentScoreboard.Count == 0) failures.Add("Client scoreboard is empty.");
   }

   private static void WriteReport(
      string outDir,
      List<string> reportLines,
      List<string> failures,
      Action<int> setExitCode)
   {
      reportLines.Add(string.Empty);

      if (failures.Count == 0)
      {
         reportLines.Add("=== SELF-TEST RESULT: ALL CHECKS PASSED ===");
         setExitCode(0);
      }
      else
      {
         reportLines.Add($"=== SELF-TEST RESULT: FAILED ({failures.Count} issue(s)) ===");
         foreach (var f in failures) reportLines.Add($"  [FAIL] {f}");
         setExitCode(1);
      }

      var reportText = string.Join(Environment.NewLine, reportLines);
      File.WriteAllText(Path.Combine(outDir, "selftest_report.txt"), reportText);
      Console.WriteLine(reportText);
   }

   private static (double Mean, double Cv) ComputeSpeedStats(List<double> speeds)
   {
      if (speeds.Count < 3) return (0, 1);

      var mean = speeds.Average();
      if (Math.Abs(mean) < 1e-3) return (0, 1);

      var variance = speeds.Sum(v => (v - mean) * (v - mean)) / speeds.Count;
      return (mean, Math.Sqrt(variance) / Math.Abs(mean));
   }
}

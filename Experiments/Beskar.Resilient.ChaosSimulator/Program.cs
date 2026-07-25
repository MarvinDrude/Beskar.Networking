using System.Buffers;
using System.Net;
using System.Net.Quic;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;
using Beskar.Utilities.Console.Rendering;
using Beskar.Utilities.Tracing;

namespace Beskar.Resilient.ChaosSimulator;

public static class Program
{
   private const int ServerPortTcp = 5000;
   private const int ServerPortWs = 5080;
   private const int ServerPortQuic = 5880;

   internal static long ServerConnectionsTotal;
   internal static long ServerConnectionsGraceful;
   internal static long ServerConnectionsAbrupt;
   internal static long ServerMessagesTotal;

   internal static long ClientAttempts;
   internal static long ClientConnectSuccess;
   internal static long ClientConnectFailUnexpected;
   internal static long ClientMessagesSent;
   internal static long ClientMessagesReceived;
   internal static long ClientPingsSent;

   internal static long ActiveTcpConnections;
   internal static long ActiveWsConnections;
   internal static long ActiveQuicConnections;

   internal static readonly Lock LogLock = new();
   internal static bool IsQuietMode { get; set; }
   internal static int Profile { get; set; } = 1; // 1 = Standard Chaos, 2 = High Throughput / Stable
   internal static int TargetConcurrentClients { get; set; } = 100;
   internal static int StatsIntervalSeconds { get; set; } = 5;

   public static async Task Main(string[] args)
   {
      TraceLogger.IsEnabled = false;

      IsQuietMode = true;

      try
      {
         Console.Clear();
      }
      catch (Exception)
      {
         // ignored
      }

      ConsoleRender.DrawHeader("BESKAR RESILIENT CHAOS SIMULATOR",
         "Simulating high load, multiple transports, and random client disconnects");

      Console.WriteLine("Select Simulator Profile:");
      Console.WriteLine("  1. Standard Chaos (Mixed roles, frequent disconnects)");
      Console.WriteLine("  2. High Throughput / Low Disconnects (Stable high-speed transmission)");
      Profile = PromptInt("Profile", Profile);
      TargetConcurrentClients = PromptInt("Number of clients", TargetConcurrentClients);
      StatsIntervalSeconds = PromptInt("Stats reporting interval (seconds)", StatsIntervalSeconds);

      Console.WriteLine();

      if (IsQuietMode)
         ConsoleRender.Success(
            "Quiet Mode enabled. Only logging exceptions/errors and periodic ASCII stats dashboard.");
      ConsoleRender.Info($"Selected Profile: {(Profile == 2 ? "High Throughput / Low Disconnects" : "Standard Chaos")}");
      ConsoleRender.Info($"Configured target concurrent clients: {TargetConcurrentClients}");
      ConsoleRender.Info($"Configured stats reporting interval: {StatsIntervalSeconds}s");

      var isQuicSupported = QuicConnection.IsSupported;
      if (!isQuicSupported)
         ConsoleRender.Warning("QUIC is not supported on this platform/OS. QUIC simulation will be disabled.");
      else
         ConsoleRender.Success("QUIC transport is supported and enabled.");

      ConsoleRender.Info("Starting Resilient Server...");
      var serverOptions = new ResilientServerOptions
      {
         FrameReceivedAllPackets = true
      };

      var serverBuilder = ResilientServerFactory.CreateBuilder(serverOptions)
         .UseTcp(ServerPortTcp)
         .UseWs(ServerPortWs);

      if (isQuicSupported) serverBuilder.UseQuic(ServerPortQuic);

      var server = serverBuilder.Build();
      RegisterServerEventHandlers(server);

      var startResult = await server.StartAsync();
      if (startResult.Failed)
      {
         ConsoleRender.Error($"Resilient Server failed to start: {startResult.Error.Detail}");
         return;
      }

      ConsoleRender.Success(
         $"Resilient Server listening on TCP:{ServerPortTcp}, WS:{ServerPortWs}{(isQuicSupported ? $", QUIC:{ServerPortQuic}" : "")}");

      // Start Chaos Client horde
      ConsoleRender.Info($"Launching chaos engine with target {TargetConcurrentClients} concurrent clients...");
      using var cts = new CancellationTokenSource();
      var clientTasks = new List<Task>();

      for (var i = 0; i < TargetConcurrentClients; i++)
      {
         var clientIndex = i;
         clientTasks.Add(Task.Run(() => ClientLifecycleLoopAsync(clientIndex, isQuicSupported, cts.Token)));
      }

      // Start Statistics Reporting Task
      var reporterTask = Task.Run(() => StatsReporter.RunStatsReporterAsync(cts.Token));

      ConsoleRender.Info("Simulator is running. Press Enter to stop...");
      if (Console.IsInputRedirected)
      {
         try
         {
            await Task.Delay(15000, cts.Token);
         }
         catch (TaskCanceledException) { }
      }
      else
      {
         Console.ReadLine();
      }

      ConsoleRender.Warning("Shutting down simulator...");
      await cts.CancelAsync();

      // Await all client scenarios and reporter to terminate
      try
      {
         await Task.WhenAll(clientTasks);
         await reporterTask;
      }
      catch (Exception)
      {
         // Ignored cancellation exceptions
      }

      // Stop server
      ConsoleRender.Info("Stopping Resilient Server...");
      await server.StopAsync();
      await server.DisposeAsync();

      ConsoleRender.Success("Simulator stopped successfully.");
   }

   private static void RegisterServerEventHandlers(ResilientServer<BeskarPacket> server)
   {
      // Client Connect Handshake Event
      server.Events.OnConnect.Add((ctx, ct) =>
      {
         var client = ctx.Client;
         var transport = client.Session.Transport;
         var clientId = client.Id.ToString()[..8];

         Interlocked.Increment(ref ServerConnectionsTotal);

         if (transport == TransportKind.Tcp) Interlocked.Increment(ref ActiveTcpConnections);
         else if (transport == TransportKind.WebSocket) Interlocked.Increment(ref ActiveWsConnections);
         else if (transport == TransportKind.Quic) Interlocked.Increment(ref ActiveQuicConnections);

         LogChaos("SERVER", transport.ToString().ToUpper(), "CONNECT",
            $"Client '{clientId}' connected successfully.", ConsoleColor.Green);
         return ValueTask.CompletedTask;
      });

      // Client Disconnected Event
      server.Events.ClientDisconnected.Add((ctx, ct) =>
      {
         var client = ctx.Client;
         if (!client.IsHandshakeCompleted)
         {
            return ValueTask.CompletedTask;
         }

         var transport = client.Session.Transport;
         var clientId = client.Id.ToString()[..8];

         if (transport == TransportKind.Tcp) Interlocked.Decrement(ref ActiveTcpConnections);
         else if (transport == TransportKind.WebSocket) Interlocked.Decrement(ref ActiveWsConnections);
         else if (transport == TransportKind.Quic) Interlocked.Decrement(ref ActiveQuicConnections);

         // Resilient ServerClient doesn't distinguish disconnect kinds on Server level,
         // but if the client has DisconnectPayload, it disconnected gracefully.
         if (client.DisconnectPayload != null)
         {
            Interlocked.Increment(ref ServerConnectionsGraceful);
            LogChaos("SERVER", transport.ToString().ToUpper(), "DISCONNECT",
               $"Client '{clientId}' disconnected gracefully. Reason: {client.DisconnectPayload.ReasonString}", ConsoleColor.Gray);
         }
         else
         {
            Interlocked.Increment(ref ServerConnectionsAbrupt);
            LogChaos("SERVER", transport.ToString().ToUpper(), "DISCONNECT",
               $"Client '{clientId}' disconnected abruptly!", ConsoleColor.Red, true);
         }

         return ValueTask.CompletedTask;
      });

      // Message Received Event
      server.Events.FrameReceived.Add((ctx, ct) =>
      {
         var kind = ctx.Frame.GetFrameKind();
         if (kind == ResilientFrameKind.Message)
         {
            Interlocked.Increment(ref ServerMessagesTotal);
         }
         return ValueTask.CompletedTask;
      });
   }

   private static async Task ClientLifecycleLoopAsync(int clientIndex, bool isQuicSupported, CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            await RunClientScenarioAsync(clientIndex, isQuicSupported, ct);
         }
         catch (Exception ex)
         {
            // Catch all scenario exceptions to keep the simulation alive
            LogChaos("CLIENT", "ERR", "SCENARIO_ERR",
               $"Unexpected error in lifecycle of client {clientIndex}: {ex.Message}", ConsoleColor.DarkRed, true);
         }

         // Random delay before spawning next client in this slot
         await Task.Delay(Random.Shared.Next(1000, 4000), ct);
      }
   }

   private static async Task RunClientScenarioAsync(int clientIndex, bool isQuicSupported, CancellationToken ct)
   {
      // Select Transport
      var transports = new List<TransportKind> { TransportKind.Tcp, TransportKind.WebSocket };
      if (isQuicSupported) transports.Add(TransportKind.Quic);
      var transport = transports[Random.Shared.Next(transports.Count)];

      // Select Client Role:
      ClientRole role;
      if (Profile == 2)
      {
         var roll = Random.Shared.Next(100);
         role = roll switch
         {
            < 40 => ClientRole.Sender,
            < 80 => ClientRole.Echoer,
            _ => ClientRole.ChannelCongestor
         };
      }
      else
      {
         var roleRoll = Random.Shared.Next(100);
         role = roleRoll switch
         {
            < 25 => ClientRole.Sender,
            < 50 => ClientRole.Echoer,
            < 65 => ClientRole.KeepAliveOnly,
            < 80 => ClientRole.Flaky,
            < 90 => ClientRole.SlowReceiver,
            _ => ClientRole.ChannelCongestor
         };
      }

      var clientIdStr = $"chaos-{role.ToString().ToLower()}-{clientIndex}";

      // Instantiate Client
      var port = transport switch
      {
         TransportKind.Tcp => ServerPortTcp,
         TransportKind.WebSocket => ServerPortWs,
         TransportKind.Quic => ServerPortQuic,
         _ => throw new InvalidOperationException()
      };

      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      };

      // Set custom keep-alive for KeepAliveOnly role
      if (role == ClientRole.KeepAliveOnly)
      {
         clientOptions.KeepAlive = new ResilientClientKeepAliveOptions
         {
            Enabled = true,
            KeepAliveInterval = TimeSpan.FromSeconds(3)
         };
      }

      await using var client = transport switch
      {
         TransportKind.Tcp => ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions),
         TransportKind.WebSocket => ResilientClientFactory.CreateWs<BeskarPacket>(clientOptions: clientOptions),
         TransportKind.Quic => ResilientClientFactory.CreateQuic<BeskarPacket>(clientOptions: clientOptions),
         _ => throw new InvalidOperationException()
      };

      Interlocked.Increment(ref ClientAttempts);
      var transportStr = transport.ToString().ToUpper();

      LogChaos("CLIENT", transportStr, "CONNECTING",
         $"Client '{clientIdStr}' connecting...", ConsoleColor.DarkYellow);

      Result<VoidResult<StringError>, StringError> connectResult;
      try
      {
         using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
         connectCts.CancelAfter(TimeSpan.FromSeconds(5));
         connectResult = await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port), connectCts.Token);
      }
      catch (Exception ex)
      {
         connectResult = new StringError($"Connection failed: {ex.Message}");
      }

      if (connectResult.Failed)
      {
         Interlocked.Increment(ref ClientConnectFailUnexpected);
         LogChaos("CLIENT", transportStr, "CONN_ERR",
            $"Client '{clientIdStr}' connection failed: {connectResult.Error.Detail}", ConsoleColor.Red, true);
         return;
      }

      Interlocked.Increment(ref ClientConnectSuccess);
      LogChaos("CLIENT", transportStr, "CONN_OK", $"Client '{clientIdStr}' connected successfully.",
         ConsoleColor.Green);

      // Execute role behavior
      try
      {
         switch (role)
         {
            case ClientRole.Sender:
               await ClientBehaviors.ExecuteSenderBehaviorAsync(client, clientIdStr, transportStr, ct);
               break;

            case ClientRole.Echoer:
               await ClientBehaviors.ExecuteEchoerBehaviorAsync(client, clientIdStr, transportStr, ct);
               break;

            case ClientRole.KeepAliveOnly:
               await ClientBehaviors.ExecuteKeepAliveBehaviorAsync(client, clientIdStr, transportStr, ct);
               break;

            case ClientRole.Flaky:
               await ClientBehaviors.ExecuteFlakyBehaviorAsync(client, clientIdStr, transportStr, ct);
               break;

            case ClientRole.SlowReceiver:
               await ClientBehaviors.ExecuteSlowReceiverBehaviorAsync(client, clientIdStr, transportStr, ct);
               break;

            case ClientRole.ChannelCongestor:
               await ClientBehaviors.ExecuteChannelCongestorBehaviorAsync(client, clientIdStr, transportStr, ct);
               break;
         }
      }
      catch (Exception ex)
      {
         LogChaos("CLIENT", transportStr, "ERROR",
            $"Client '{clientIdStr}' threw error: {ex.Message}", ConsoleColor.DarkRed, true);
      }
      finally
      {
         // Disconnect (Decide Graceful vs Abrupt)
         var gracefulChance = Profile == 2 ? 98 : 85;
         var disconnectGraceful = Random.Shared.Next(100) < gracefulChance;

         if (disconnectGraceful)
         {
            LogChaos("CLIENT", transportStr, "DISCONN_G",
               $"Client '{clientIdStr}' disconnecting gracefully...", ConsoleColor.DarkGray);
            try
            {
               using var disconnectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
               disconnectCts.CancelAfter(TimeSpan.FromSeconds(2));

               var disconnectPayload = new DisconnectPacketPayload
               {
                  ReasonCode = 0,
                  ReasonString = "Graceful chaos disconnect"
               };
               await client.DisconnectAsync(disconnectPayload);
            }
            catch
            {
               /* Ignored */
            }
         }
         else
         {
            LogChaos("CLIENT", transportStr, "DISCONN_A",
               $"Client '{clientIdStr}' disconnecting abruptly (crash sim)!", ConsoleColor.Red);
            // Abrupt disconnect: skip DisconnectAsync and dispose client directly.
         }
      }
   }

   internal static void LogChaos(string source, string transport, string eventName, string message,
      ConsoleColor? color = null, bool isError = false)
   {
      if (IsQuietMode && !isError) return;

      lock (LogLock)
      {
         var tagColorName = color?.ToString() ?? "Gray";
         var markup =
            $"[darkgray][{DateTime.Now:HH:mm:ss}][/] [[{tagColorName}]{source,-6}[/]] [[cyan]{transport,-5}[/]] [[yellow]{eventName,-10}[/]] {message}";
         ConsoleRender.WriteMarkupLine(markup);
      }
   }

   private static int PromptInt(string prompt, int defaultValue)
   {
      Console.Write($"{prompt} [default: {defaultValue}]: ");
      var input = Console.ReadLine();
      if (string.IsNullOrWhiteSpace(input)) return defaultValue;

      if (int.TryParse(input, out var value)) return value;

      Console.ForegroundColor = ConsoleColor.Red;
      Console.WriteLine($"Invalid input, using default value: {defaultValue}");
      Console.ResetColor();
      return defaultValue;
   }
}

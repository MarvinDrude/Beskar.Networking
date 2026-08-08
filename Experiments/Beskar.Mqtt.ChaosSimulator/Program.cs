using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Quic;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Common.Telemetry;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Enums;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Resilient.Common.Telemetry;
using Beskar.Utilities.Console.Rendering;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.ChaosSimulator;

public static class Program
{
   internal static readonly ConcurrentDictionary<string, long> TelemetryGauges = new();
   internal static readonly ConcurrentDictionary<string, long> TelemetryCounters = new();
   private static readonly MeterListener MeterListener = new();
   private const int ServerPortTcp = 1883;
   private const int ServerPortWs = 8083;
   private const int ServerPortQuic = 8883;

   internal static long ServerConnectionsTotal;
   internal static long ServerConnectionsGraceful;
   internal static long ServerConnectionsAbrupt;
   internal static long ServerAuthV3Success;
   internal static long ServerAuthV3Failure;
   internal static long ServerAuthV5Success;
   internal static long ServerAuthV5Failure;
   internal static long ServerPublishesQoS0 = 0;
   internal static long ServerPublishesQoS1;
   internal static long ServerPublishesQoS2;
   internal static long ServerPublishesTotal;
   internal static long ServerNoSubscriberMessages;
   internal static long ServerSubscriptions;
   internal static long ServerUnsubscriptions;

   internal static long ClientAttempts;
   internal static long ClientConnectSuccess;
   internal static long ClientConnectFailExpected;
   internal static long ClientConnectFailUnexpected;
   internal static long ClientPublishesSent = 0;
   internal static long ClientPublishesFailed = 0;
   internal static long ClientMessagesReceived = 0;
   internal static long ClientPingsSent = 0;

   internal static long ActiveTcpConnections;
   internal static long ActiveWsConnections;
   internal static long ActiveQuicConnections;

   internal static readonly Lock LogLock = new();
   internal static bool IsQuietMode { get; set; }
   internal static int TransportMode { get; set; } = 0; // 0 = Mixed, 1 = QUIC Only, 2 = TCP Only, 3 = WS Only
   internal static int TargetConcurrentClients { get; set; } = 20;
   internal static int StatsIntervalSeconds { get; set; } = 10;

   public static async Task Main(string[] args)
   {
      TraceLogger.IsEnabled = false;

      IsQuietMode = true;
      TargetConcurrentClients = 200;
      StatsIntervalSeconds = 5;

      MeterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name is TransportMetrics.MeterName or ResilientMetrics.MeterName or MqttMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };

      MeterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         var name = instrument.Name;
         if (instrument is UpDownCounter<long>)
         {
            TelemetryGauges.AddOrUpdate(name, measurement, (_, prev) => prev + measurement);
         }
         else if (instrument is Counter<long>)
         {
            TelemetryCounters.AddOrUpdate(name, measurement, (_, prev) => prev + measurement);
         }
      });

      MeterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
      {
         var name = instrument.Name;
         TelemetryGauges.AddOrUpdate(name, (long)measurement, (_, _) => (long)measurement);
      });

      MeterListener.Start();

      try
      {
         Console.Clear();
      }
      catch (Exception)
      {
         // ignored
      }

      ConsoleRender.DrawHeader("BESKAR MQTT CHAOS SIMULATOR",
         "Simulating high load, multiple transports, authentication & random disconnections");

      if (IsQuietMode)
         ConsoleRender.Success(
            "Quiet Mode enabled. Only logging exceptions/errors and periodic ASCII stats dashboard.");
      ConsoleRender.Info($"Configured target concurrent clients: {TargetConcurrentClients}");
      ConsoleRender.Info($"Configured stats reporting interval: {StatsIntervalSeconds}s");

      var isQuicSupported = QuicConnection.IsSupported;
      if (isQuicSupported)
      {
         Console.WriteLine("Select Transport Mode:");
         Console.WriteLine("  0. Mixed Transports (TCP, WS, QUIC)");
         Console.WriteLine("  1. QUIC ONLY");
         Console.WriteLine("  2. TCP ONLY");
         Console.WriteLine("  3. WebSocket ONLY");
         TransportMode = PromptInt("Transport Mode", 0);

         if (TransportMode == 1) ConsoleRender.Success("QUIC ONLY mode selected for all clients.");
         else if (TransportMode == 2) ConsoleRender.Info("TCP ONLY mode selected for all clients.");
         else if (TransportMode == 3) ConsoleRender.Info("WebSocket ONLY mode selected for all clients.");
      }
      else
      {
         ConsoleRender.Warning("QUIC is not supported on this platform/OS. Defaulting to TCP & WS.");
      }

      ConsoleRender.Info("Starting MQTT Server...");
      var serverBuilder = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(ServerPortTcp)
         .UseWs(ServerPortWs);

      if (isQuicSupported) serverBuilder.UseQuic(ServerPortQuic);

      var server = serverBuilder.Build();
      RegisterServerEventHandlers(server);

      var startResult = await server.StartAsync();
      if (startResult.Failed)
      {
         ConsoleRender.Error($"MQTT Server failed to start: {startResult.Error.Detail}");
         return;
      }

      ConsoleRender.Success(
         $"MQTT Server listening on TCP:{ServerPortTcp}, WS:{ServerPortWs}{(isQuicSupported ? $", QUIC:{ServerPortQuic}" : "")}");

      // 2. Start Chaos Client horde
      ConsoleRender.Info($"Launching chaos engine with target {TargetConcurrentClients} concurrent clients...");
      using var cts = new CancellationTokenSource();
      var clientTasks = new List<Task>();

      for (var i = 0; i < TargetConcurrentClients; i++)
      {
         var clientIndex = i;
         clientTasks.Add(Task.Run(() => ClientLifecycleLoopAsync(clientIndex, isQuicSupported, cts.Token)));
      }

      // 3. Start Statistics Reporting Task
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
      ConsoleRender.Info("Stopping MQTT Server...");
      await server.StopAsync();
      await server.DisposeAsync();

      ConsoleRender.Success("Simulator stopped successfully.");
   }

   private static void RegisterServerEventHandlers(MqttServer server)
   {
      // Authentication Interceptor (v3 simple auth, v5 challenge-response)
      server.Events.OnConnectIntercept.Add(async (ctx, ct) =>
      {
         var protocolVersion = ctx.ConnectOptions.ProtocolVersion;
         var clientId = Encoding.UTF8.GetString(ctx.ConnectOptions.ClientIdUtf8Bytes.Span);
         if (string.IsNullOrEmpty(clientId))
            clientId = ctx.AssignedClientIdentifierUtf8Bytes.Length > 0
               ? Encoding.UTF8.GetString(ctx.AssignedClientIdentifierUtf8Bytes.Span)
               : "assigned-id";

         if (protocolVersion is MqttProtocolVersion.V311 or MqttProtocolVersion.V31)
         {
            var username = ctx.ConnectOptions.UsernameUtf8Bytes.IsEmpty
               ? string.Empty
               : Encoding.UTF8.GetString(ctx.ConnectOptions.UsernameUtf8Bytes.Span);

            var password = ctx.ConnectOptions.PasswordBytes.IsEmpty
               ? string.Empty
               : Encoding.UTF8.GetString(ctx.ConnectOptions.PasswordBytes.Span);

            if (username == "admin" && password == "secret")
            {
               ctx.ReasonCode = ConnectReasonCode.Success;
               Interlocked.Increment(ref ServerAuthV3Success);
            }
            else
            {
               ctx.ReasonCode = ConnectReasonCode.BadUserNameOrPassword;
               ctx.ReasonString = "Invalid username or password";
               Interlocked.Increment(ref ServerAuthV3Failure);
               LogChaos("SERVER", "CONN", "v3", "AUTH_FAIL",
                  $"Rejected client '{clientId}' due to incorrect credentials.", ConsoleColor.Red, true);
            }
         }
         else if (protocolVersion == MqttProtocolVersion.V50)
         {
            var authMethod = ctx.ConnectOptions.AuthenticationMethodUtf8Bytes.ToArray();
            var expectedMethod1 = "ChallengeResponse"u8.ToArray();
            var expectedMethod2 = new byte[] { 2, 3, 4 };

            var isMethodMatch = authMethod.SequenceEqual(expectedMethod1) || authMethod.SequenceEqual(expectedMethod2);

            if (!isMethodMatch)
            {
               ctx.ReasonCode = ConnectReasonCode.BadAuthenticationMethod;
               ctx.ReasonString = "Unsupported authentication method.";
               Interlocked.Increment(ref ServerAuthV5Failure);
               LogChaos("SERVER", "CONN", "v5", "AUTH_FAIL",
                  $"Rejected client '{clientId}' due to unsupported auth method.", ConsoleColor.Red, true);
               return;
            }

            var initialData = ctx.ConnectOptions.AuthenticationDataBytes.ToArray();
            var expectedInitialData = new byte[] { 2, 3, 4 };

            if (!initialData.SequenceEqual(expectedInitialData))
            {
               ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
               ctx.ReasonString = "Invalid initial authentication data";
               Interlocked.Increment(ref ServerAuthV5Failure);
               LogChaos("SERVER", "CONN", "v5", "AUTH_FAIL",
                  $"Rejected client '{clientId}' due to invalid initial auth data.", ConsoleColor.Red, true);
               return;
            }

            // Challenge-Response Loop
            var challengeBytes = new byte[] { 10, 20, 30 };
            var challengePacket = new AuthPacket
            {
               ReasonCode = AuthenticateReasonCode.ContinueAuthentication,
               AuthenticationMethodUtf8Bytes =
                  new ReadOnlySequence<byte>(ctx.ConnectOptions.AuthenticationMethodUtf8Bytes),
               AuthenticationDataBytes = new ReadOnlySequence<byte>(challengeBytes),
               ReasonUtf8Bytes = new ReadOnlySequence<byte>([.. "Challenge"u8])
            };

            LogChaos("SERVER", "CONN", "v5", "CHALLENGE",
               $"Sending enhanced authentication challenge to '{clientId}'...", ConsoleColor.DarkYellow);
            await ctx.SendAuthPacketAsync(new AuthPacketOptions(challengePacket), ct);

            // Await client response
            var response = await ctx.ReceiveControlPacketAsync(ct);

            if (response is not AuthPacketOptions clientAuthOptions)
            {
               ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
               ctx.ReasonString = "Expected AUTH packet from client";
               Interlocked.Increment(ref ServerAuthV5Failure);
               LogChaos("SERVER", "CONN", "v5", "AUTH_FAIL",
                  $"Rejected client '{clientId}': missing expected AUTH packet response.", ConsoleColor.Red, true);
               return;
            }

            var clientResponseData = clientAuthOptions.AuthenticationDataBytes.ToArray();
            var expectedResponse = new byte[] { 11, 21, 31 }; // challengeBytes + 1

            if (!clientResponseData.SequenceEqual(expectedResponse))
            {
               ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
               ctx.ReasonString = "Invalid challenge response";
               Interlocked.Increment(ref ServerAuthV5Failure);
               LogChaos("SERVER", "CONN", "v5", "AUTH_FAIL",
                  $"Rejected client '{clientId}': invalid challenge response solution.", ConsoleColor.Red, true);
               return;
            }

            ctx.ReasonCode = ConnectReasonCode.Success;
            ctx.ResponseAuthenticationData = "AuthSuccess"u8.ToArray();
            Interlocked.Increment(ref ServerAuthV5Success);
         }
         else
         {
            ctx.ReasonCode = ConnectReasonCode.UnsupportedProtocolVersion;
            ctx.ReasonString = "Unsupported protocol version";
         }
      });

      // Session Connected Event
      server.Events.OnConnect.Add((ctx, ct) =>
      {
         var client = ctx.Client;
         var transport = client.Session.Transport;
         var version = client.ProtocolVersion == MqttProtocolVersion.V50 ? "v5" : "v3";
         var clientId = Encoding.UTF8.GetString(client.ClientIdUtf8Bytes.Span);

         Interlocked.Increment(ref ServerConnectionsTotal);

         if (transport == TransportKind.Tcp) Interlocked.Increment(ref ActiveTcpConnections);
         else if (transport == TransportKind.WebSocket) Interlocked.Increment(ref ActiveWsConnections);
         else if (transport == TransportKind.Quic) Interlocked.Increment(ref ActiveQuicConnections);

         LogChaos("SERVER", transport.ToString().ToUpper(), version, "CONNECT",
            $"Client '{clientId}' connected successfully.", ConsoleColor.Green);
         return ValueTask.CompletedTask;
      });

      // Session Disconnected Event
      server.Events.OnDisconnect.Add((ctx, ct) =>
      {
         var client = ctx.ServerClient;
         if (client.MqttSession is null)
         {
            // Failed connect intercept/auth phase, never successfully established a session.
            return ValueTask.CompletedTask;
         }

         var transport = client.Session.Transport;
         var version = client.ProtocolVersion == MqttProtocolVersion.V50 ? "v5" : "v3";
         var clientId = Encoding.UTF8.GetString(client.ClientIdUtf8Bytes.Span);

         if (transport == TransportKind.Tcp) Interlocked.Decrement(ref ActiveTcpConnections);
         else if (transport == TransportKind.WebSocket) Interlocked.Decrement(ref ActiveWsConnections);
         else if (transport == TransportKind.Quic) Interlocked.Decrement(ref ActiveQuicConnections);

         var disconnectKind = ctx.DisconnectKind;
         if (disconnectKind == ClientDisconnectKind.Graceful)
         {
            Interlocked.Increment(ref ServerConnectionsGraceful);
            LogChaos("SERVER", transport.ToString().ToUpper(), version, "DISCONNECT",
               $"Client '{clientId}' disconnected gracefully.", ConsoleColor.Gray);
         }
         else
         {
            Interlocked.Increment(ref ServerConnectionsAbrupt);
            LogChaos("SERVER", transport.ToString().ToUpper(), version, "DISCONNECT",
               $"Client '{clientId}' disconnected abruptly! Reason: {ctx.Reason}", ConsoleColor.Red, true);
         }

         return ValueTask.CompletedTask;
      });

      // Message Publish Acknowledged (QoS 1 / 2)
      server.Events.OnAcknowledgePub.Add((ctx, ct) =>
      {
         var qos = ctx.PublishMessage.QualityOfService;
         Interlocked.Increment(ref ServerPublishesTotal);

         if (qos == QualityOfServiceType.AtLeastOnce)
            Interlocked.Increment(ref ServerPublishesQoS1);
         else if (qos == QualityOfServiceType.ExactlyOnce) Interlocked.Increment(ref ServerPublishesQoS2);

         return ValueTask.CompletedTask;
      });

      // No Subscriber Event
      server.Events.OnNoSubscriberMessage.Add((ctx, ct) =>
      {
         Interlocked.Increment(ref ServerNoSubscriberMessages);
         return ValueTask.CompletedTask;
      });

      // Subscription Hooks
      server.Events.OnSubscribe.Add((ctx, ct) =>
      {
         var clientId = Encoding.UTF8.GetString(ctx.Session.ClientIdUtf8Bytes);
         var transport = ctx.Session.Client?.Session?.Transport ?? TransportKind.Unknown;
         var version = ctx.Session.Client?.ProtocolVersion == MqttProtocolVersion.V50 ? "v5" : "v3";

         Interlocked.Increment(ref ServerSubscriptions);
         LogChaos("SERVER", transport.ToString().ToUpper(), version, "SUBSCRIBE",
            $"Client '{clientId}' subscribed to '{ctx.TopicFilter}' (QoS {ctx.QualityOfService})", ConsoleColor.Blue);
         return ValueTask.CompletedTask;
      });

      server.Events.OnUnsubscribe.Add((ctx, ct) =>
      {
         var clientId = Encoding.UTF8.GetString(ctx.Session.ClientIdUtf8Bytes);
         var transport = ctx.Session.Client?.Session?.Transport ?? TransportKind.Unknown;
         var version = ctx.Session.Client?.ProtocolVersion == MqttProtocolVersion.V50 ? "v5" : "v3";

         Interlocked.Increment(ref ServerUnsubscriptions);
         LogChaos("SERVER", transport.ToString().ToUpper(), version, "UNSUBSCRIBE",
            $"Client '{clientId}' unsubscribed.", ConsoleColor.Cyan);
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
            LogChaos("CLIENT", "ERR", "N/A", "SCENARIO_ERR",
               $"Unexpected error in lifecycle of client {clientIndex}: {ex.Message}", ConsoleColor.DarkRed, true);
         }

         // Random delay before spawning next client in this slot
         await Task.Delay(Random.Shared.Next(1000, 4000), ct);
      }
   }

   private static async Task RunClientScenarioAsync(int clientIndex, bool isQuicSupported, CancellationToken ct)
   {
      // Select Transport
      var transports = new List<TransportKind>();
      if (TransportMode == 1 && isQuicSupported)
      {
         transports.Add(TransportKind.Quic);
      }
      else if (TransportMode == 2)
      {
         transports.Add(TransportKind.Tcp);
      }
      else if (TransportMode == 3)
      {
         transports.Add(TransportKind.WebSocket);
      }
      else
      {
         transports.Add(TransportKind.Tcp);
         transports.Add(TransportKind.WebSocket);
         if (isQuicSupported) transports.Add(TransportKind.Quic);
      }
      var transport = transports[Random.Shared.Next(transports.Count)];

      // Select Version
      var version = Random.Shared.Next(2) == 0 ? MqttProtocolVersion.V311 : MqttProtocolVersion.V50;

      // Select Client Role:
      // 20% Publisher, 20% Subscriber, 10% KeepAliveOnly, 10% Flaky, 10% SlowSubscriber,
      // 10% Qos2HeavyPublisher, 10% WildcardSubscriber, 5% AuthAlternator, 5% ChannelCongestor
      var roleRoll = Random.Shared.Next(100);
      var role = roleRoll switch
      {
         < 20 => ClientRole.Publisher,
         < 40 => ClientRole.Subscriber,
         < 50 => ClientRole.KeepAliveOnly,
         < 60 => ClientRole.Flaky,
         < 70 => ClientRole.SlowSubscriber,
         < 80 => ClientRole.Qos2HeavyPublisher,
         < 90 => ClientRole.WildcardSubscriber,
         < 95 => ClientRole.AuthAlternator,
         _ => ClientRole.ChannelCongestor
      };

      // Select Authentication Scenario
      // For AuthAlternator role, alternate scenario to generate auth failures
      var roll = Random.Shared.Next(100);
      var authScenario = role == ClientRole.AuthAlternator
         ? Random.Shared.Next(2) == 0 ? AuthScenario.Valid : AuthScenario.Invalid
         : roll switch
         {
            < 80 => AuthScenario.Valid,
            < 90 => AuthScenario.Invalid,
            _ => AuthScenario.Unauthenticated
         };

      var clientIdStr = $"chaos-{role.ToString().ToLower()}-{clientIndex}-{Guid.NewGuid().ToString()[..6]}";

      // Instantiate Client
      await using var client = transport switch
      {
         TransportKind.Tcp => MqttClientFactory.CreateTcp(),
         TransportKind.WebSocket => MqttClientFactory.CreateWs(),
         TransportKind.Quic => MqttClientFactory.CreateQuic(),
         _ => throw new InvalidOperationException()
      };

      // Setup Connect Options
      var port = transport switch
      {
         TransportKind.Tcp => ServerPortTcp,
         TransportKind.WebSocket => ServerPortWs,
         TransportKind.Quic => ServerPortQuic,
         _ => throw new InvalidOperationException()
      };

      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = version,
         ClientIdUtf8Bytes = Encoding.UTF8.GetBytes(clientIdStr),
         CleanSession = true,
         Timeout = TimeSpan.FromSeconds(5),
         KeepAlivePeriod = (ushort)(role == ClientRole.KeepAliveOnly ? 5 : 60)
      };

      // Configure Credentials/Auth
      if (version == MqttProtocolVersion.V50)
      {
         if (authScenario == AuthScenario.Valid)
         {
            connectOptions.AuthenticationMethodUtf8Bytes = "ChallengeResponse"u8.ToArray();
            connectOptions.AuthenticationDataBytes = new byte[] { 2, 3, 4 };
            connectOptions.AuthenticationHandler = new AuthHandler(true);
         }
         else if (authScenario == AuthScenario.Invalid)
         {
            connectOptions.AuthenticationMethodUtf8Bytes = "ChallengeResponse"u8.ToArray();
            connectOptions.AuthenticationDataBytes = new byte[] { 2, 3, 4 };
            connectOptions.AuthenticationHandler = new AuthHandler(false);
         }
      }
      else // v3.1.1
      {
         if (authScenario == AuthScenario.Valid)
         {
            connectOptions.UsernameUtf8Bytes = "admin"u8.ToArray();
            connectOptions.PasswordBytes = "secret"u8.ToArray();
         }
         else if (authScenario == AuthScenario.Invalid)
         {
            connectOptions.UsernameUtf8Bytes = "admin"u8.ToArray();
            connectOptions.PasswordBytes = "wrong-pwd"u8.ToArray();
         }
      }

      Interlocked.Increment(ref ClientAttempts);
      var versionStr = version == MqttProtocolVersion.V50 ? "v5" : "v3";
      var transportStr = transport.ToString().ToUpper();

      LogChaos("CLIENT", transportStr, versionStr, "CONNECTING",
         $"Client '{clientIdStr}' connecting (Auth scenario: {authScenario})...", ConsoleColor.DarkYellow);

       Result<ClientConnectResult, StringError> connectResult;
       try
       {
           using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
           connectCts.CancelAfter(TimeSpan.FromSeconds(15));
           connectResult = await client.ConnectAsync(connectOptions, connectCts.Token);
       }
       catch (Exception ex)
       {
          connectResult = new StringError($"Connection timed out or failed: {ex.Message}");
       }

      if (connectResult.Failed)
      {
         if (authScenario != AuthScenario.Valid)
         {
            Interlocked.Increment(ref ClientConnectFailExpected);
            LogChaos("CLIENT", transportStr, versionStr, "CONN_FAIL",
               $"Client '{clientIdStr}' connection failed as expected: {connectResult.Error.Detail}",
               ConsoleColor.DarkGray);
         }
         else
         {
            Interlocked.Increment(ref ClientConnectFailUnexpected);
            LogChaos("CLIENT", transportStr, versionStr, "CONN_ERR",
               $"Client '{clientIdStr}' connection failed unexpectedly: {connectResult.Error.Detail}", ConsoleColor.Red,
               true);
         }

         return;
      }

      Interlocked.Increment(ref ClientConnectSuccess);
      LogChaos("CLIENT", transportStr, versionStr, "CONN_OK", $"Client '{clientIdStr}' connected successfully.",
         ConsoleColor.Green);

      // Execute role behavior
      try
      {
         switch (role)
         {
            case ClientRole.Publisher:
               await ClientBehaviors.ExecutePublisherBehaviorAsync(client, clientIdStr, transportStr, versionStr, ct);
               break;

            case ClientRole.Subscriber:
               await ClientBehaviors.ExecuteSubscriberBehaviorAsync(client, clientIdStr, transportStr, versionStr, ct);
               break;

            case ClientRole.KeepAliveOnly:
               await ClientBehaviors.ExecuteKeepAliveBehaviorAsync(client, clientIdStr, transportStr, versionStr, ct);
               break;

            case ClientRole.Flaky:
               await ClientBehaviors.ExecuteFlakyBehaviorAsync(client, clientIdStr, transportStr, versionStr, ct);
               break;

            case ClientRole.SlowSubscriber:
               await ClientBehaviors.ExecuteSlowSubscriberBehaviorAsync(client, clientIdStr, transportStr, versionStr,
                  ct);
               break;

            case ClientRole.Qos2HeavyPublisher:
               await ClientBehaviors.ExecuteQos2HeavyPublisherBehaviorAsync(client, clientIdStr, transportStr,
                  versionStr, ct);
               break;

            case ClientRole.WildcardSubscriber:
               await ClientBehaviors.ExecuteWildcardSubscriberBehaviorAsync(client, clientIdStr, transportStr,
                  versionStr, ct);
               break;

            case ClientRole.AuthAlternator:
               await ClientBehaviors.ExecuteAuthAlternatorBehaviorAsync(client, clientIdStr, transportStr, versionStr,
                  ct);
               break;

            case ClientRole.ChannelCongestor:
               await ClientBehaviors.ExecuteChannelCongestorBehaviorAsync(client, clientIdStr, transportStr, versionStr,
                  ct);
               break;
         }
      }
      catch (Exception ex)
      {
         LogChaos("CLIENT", transportStr, versionStr, "ERROR",
            $"Client '{clientIdStr}' threw error during execution: {ex.Message}", ConsoleColor.DarkRed, true);
      }
      finally
      {
         // Disconnect (Decide Graceful vs Abrupt)
         var disconnectGraceful = Random.Shared.Next(100) < 85;

         if (disconnectGraceful)
         {
            LogChaos("CLIENT", transportStr, versionStr, "DISCONN_G",
               $"Client '{clientIdStr}' disconnecting gracefully...", ConsoleColor.DarkGray);
             try
             {
                using var disconnectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                disconnectCts.CancelAfter(TimeSpan.FromSeconds(2));
                await client.DisconnectAsync(new DisconnectOptions(), disconnectCts.Token);
             }
             catch
             {
                /* Ignored */
             }
         }
         else
         {
            LogChaos("CLIENT", transportStr, versionStr, "DISCONN_A",
               $"Client '{clientIdStr}' disconnecting abruptly (simulating crash/disconnect)!", ConsoleColor.Red);
            // Abrupt disconnect: skip DisconnectAsync and dispose client directly.
         }
      }
   }

   internal static void LogChaos(string source, string transport, string version, string eventName, string message,
      ConsoleColor? color = null, bool isError = false)
   {
      if (IsQuietMode && !isError) return;

      lock (LogLock)
      {
         var tagColorName = color?.ToString() ?? "Gray";
         var markup =
            $"[darkgray][{DateTime.Now:HH:mm:ss}][/] [[{tagColorName}]{source,-6}[/]] [[cyan]{transport,-5}[/]] [[magenta]{version,-4}[/]] [[yellow]{eventName,-10}[/]] {message}";
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

using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Quic;
using System.Text;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Networking.Connections;

public enum ClientScenario
{
   PingPongGraceful,
   ImmediateGracefulDisconnect,
   AbruptDisconnectAfterPing,
   ImmediateAbruptDisconnect,
   ConnectionErrorSimulated
}

public interface ITestTransportFactory
{
   INetworkListener CreateListener(EndPoint localAddress);
   INetworkClient CreateClient();
}

public class TcpTestTransportFactory : ITestTransportFactory
{
   public INetworkListener CreateListener(EndPoint localAddress)
   {
      return new TcpNetworkListener(localAddress, new TcpTransportOptions());
   }

   public INetworkClient CreateClient()
   {
      return new TcpNetworkClient(new TcpTransportOptions());
   }
}

public class WsTestTransportFactory : ITestTransportFactory
{
   public INetworkListener CreateListener(EndPoint localAddress)
   {
      return new WsNetworkListener(localAddress, new WsTransportOptions());
   }

   public INetworkClient CreateClient()
   {
      return new WsNetworkClient(new WsTransportOptions());
   }
}

public class QuicTestTransportFactory : ITestTransportFactory
{
   public INetworkListener CreateListener(EndPoint localAddress)
   {
      var options = new QuicTransportOptions
      {
         AlpnProtocol = "pingpong",
         MaxInboundBidirectionalStreams = 1000
      };
      return new QuicNetworkListener(localAddress, options);
   }

   public INetworkClient CreateClient()
   {
      var options = new QuicTransportOptions
      {
         AlpnProtocol = "pingpong"
      };
      return new QuicNetworkClient(options);
   }
}

public static class Program
{
   // Statistics
   private static long _clientAttempts;
   private static long _clientConnectSuccesses;
   private static long _clientConnectFailuresExpected;
   private static long _clientConnectFailuresUnexpected;
   private static long _clientGracefulDisconnects;
   private static long _clientAbruptDisconnects;
   private static long _clientPongsReceived;
   private static long _clientStreamErrors;
   private static long _clientErrors;

   private static long _serverSessionsAccepted;
   private static long _serverSessionsActive;
   private static long _serverPingsReceived;

   public static async Task Main(string[] args)
   {
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("======================================================================");
      Console.WriteLine("            BESKAR NETWORKING CONCURRENT CONNECTIONS HARNESS          ");
      Console.WriteLine("======================================================================");
      Console.ResetColor();

      Console.WriteLine("Select transport layer to test:");
      Console.WriteLine("  1. TCP (TcpNetworkClient / TcpNetworkListener)");
      Console.WriteLine("  2. WebSocket (WsNetworkClient / WsNetworkListener)");
      Console.WriteLine("  3. QUIC (QuicNetworkClient / QuicNetworkListener)");
      Console.Write("Enter selection (1-3, default 1): ");

      var selectionStr = Console.ReadLine();
      var selection = 1;

      if (int.TryParse(selectionStr, out var parsedSelection) && parsedSelection is >= 1 and <= 3)
         selection = parsedSelection;

      if (selection == 3 && !QuicConnection.IsSupported)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine("Error: QUIC is not supported on this platform/OS version. Falling back to TCP.");
         Console.ResetColor();
         selection = 1;
      }

      ITestTransportFactory factory = selection switch
      {
         1 => new TcpTestTransportFactory(),
         2 => new WsTestTransportFactory(),
         3 => new QuicTestTransportFactory(),
         _ => throw new InvalidOperationException()
      };

      var transportName = selection switch
      {
         1 => "TCP",
         2 => "WebSocket",
         3 => "QUIC",
         _ => throw new InvalidOperationException()
      };

      Console.Write("Enter total number of client runs to execute (default 500): ");
      var clientRunsStr = Console.ReadLine();
      var totalClientRuns = 500;

      if (int.TryParse(clientRunsStr, out var parsedRuns) && parsedRuns > 0) totalClientRuns = parsedRuns;

      Console.Write("Enter concurrent client workers limit (default 50): ");
      var concurrencyStr = Console.ReadLine();
      var concurrencyLimit = 50;

      if (int.TryParse(concurrencyStr, out var parsedConcurrency) && parsedConcurrency > 0)
         concurrencyLimit = parsedConcurrency;

      var port = 18883;
      var serverEndPoint = new IPEndPoint(IPAddress.Loopback, port);

      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine($"Starting test with {transportName} transport on loopback port {port}...");
      Console.WriteLine($"Configured: {totalClientRuns} client runs, capped at {concurrencyLimit} concurrent workers.");
      Console.ResetColor();
      Console.WriteLine();

      // 3. Start Server Listener
      var serverCts = new CancellationTokenSource();
      var listener = factory.CreateListener(serverEndPoint);

      var bindResult = await listener.BindAsync();
      if (bindResult.Failed)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Server failed to bind listener: {bindResult.Error.Message}");
         Console.ResetColor();
         return;
      }

      Console.WriteLine("Server listener bound and listening.");

      // Background task to accept sessions
      var serverAcceptTask = Task.Run(async () =>
      {
         try
         {
            while (!serverCts.Token.IsCancellationRequested)
            {
               var sessionResult = await listener.AcceptSessionAsync(serverCts.Token);
               if (sessionResult.Failed) continue;

               var session = sessionResult.Success;
               Interlocked.Increment(ref _serverSessionsAccepted);
               _ = Task.Run(() => HandleServerSessionAsync(session, serverCts.Token));
            }
         }
         catch (Exception)
         {
            // Exit loop on cancellation
         }
      });

      // 4. Run Concurrent Clients
      var stopwatch = Stopwatch.StartNew();
      var random = new Random();

      // Semaphore to limit concurrency
      using var semaphore = new SemaphoreSlim(concurrencyLimit);
      var clientTasks = new Task[totalClientRuns];

      for (var i = 0; i < totalClientRuns; i++)
      {
         var roll = random.Next(100);
         var scenario = roll switch
         {
            < 55 => ClientScenario.PingPongGraceful,
            < 70 => ClientScenario.ImmediateGracefulDisconnect,
            < 85 => ClientScenario.AbruptDisconnectAfterPing,
            < 95 => ClientScenario.ImmediateAbruptDisconnect,
            _ => ClientScenario.ConnectionErrorSimulated
         };

         var taskIndex = i;
         await semaphore.WaitAsync(serverCts.Token);

         clientTasks[taskIndex] = Task.Run(async () =>
         {
            try
            {
               await RunClientScenarioAsync(factory, serverEndPoint, scenario, serverCts.Token);
            }
            catch (Exception ex)
            {
               Console.WriteLine($"Unhandled client task error: {ex.Message}");
            }
            finally
            {
               semaphore.Release();
            }
         });
      }

      // Wait for all client scenarios to complete
      await Task.WhenAll(clientTasks);
      stopwatch.Stop();

      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("----------------------------------------------------------------------");
      Console.WriteLine("                        STRESS TEST COMPLETE                          ");
      Console.WriteLine("----------------------------------------------------------------------");
      Console.ResetColor();

      // Show stats
      var totalTimeSec = stopwatch.Elapsed.TotalSeconds;
      var opsPerSec = totalClientRuns / totalTimeSec;

      Console.WriteLine($"Total Duration:         {stopwatch.Elapsed.TotalSeconds:F2} seconds");
      Console.WriteLine($"Throughput Rate:        {opsPerSec:F2} runs/sec");
      Console.WriteLine();
      Console.WriteLine("--- CLIENT STATISTICS ---");
      Console.WriteLine($"Attempts:               {Interlocked.Read(ref _clientAttempts)}");
      Console.WriteLine($"Connect Successes:      {Interlocked.Read(ref _clientConnectSuccesses)}");
      Console.WriteLine($"Graceful Disconnects:   {Interlocked.Read(ref _clientGracefulDisconnects)}");
      Console.WriteLine($"Abrupt Disconnects:     {Interlocked.Read(ref _clientAbruptDisconnects)}");
      Console.WriteLine(
         $"Expected Failures:      {Interlocked.Read(ref _clientConnectFailuresExpected)} (Closed port simulation)");
      Console.WriteLine($"Unexpected Failures:    {Interlocked.Read(ref _clientConnectFailuresUnexpected)}");
      Console.WriteLine($"Pongs Received:         {Interlocked.Read(ref _clientPongsReceived)}");
      Console.WriteLine($"Stream Failures:        {Interlocked.Read(ref _clientStreamErrors)}");
      Console.WriteLine($"Client Errors Caught:   {Interlocked.Read(ref _clientErrors)}");
      Console.WriteLine();
      Console.WriteLine("--- SERVER STATISTICS ---");
      Console.WriteLine($"Sessions Accepted:      {Interlocked.Read(ref _serverSessionsAccepted)}");
      Console.WriteLine($"Pings Processed:        {Interlocked.Read(ref _serverPingsReceived)}");
      Console.WriteLine($"Active Sessions Left:   {Interlocked.Read(ref _serverSessionsActive)}");
      Console.WriteLine("----------------------------------------------------------------------");

      // 5. Cleanup Server
      Console.WriteLine("Cleaning up server resources...");

      await serverCts.CancelAsync();
      await listener.UnbindAsync(serverCts.Token);
      await listener.DisposeAsync();

      try
      {
         await serverAcceptTask;
      }
      catch
      {
         // Ignored
      }

      serverCts.Dispose();
      Console.WriteLine("Done.");
   }

   private static async Task HandleServerSessionAsync(INetworkSession session, CancellationToken ct)
   {
      Interlocked.Increment(ref _serverSessionsActive);
      try
      {
         if (session.IsSupportingMultiplexing)
         {
            while (!ct.IsCancellationRequested && !session.SessionClosedToken.IsCancellationRequested)
            {
               var acceptStreamResult = await session.AcceptStreamAsync(ct);
               if (acceptStreamResult.Failed) break;

               var stream = acceptStreamResult.Success;
               _ = Task.Run(() => HandleServerStreamAsync(stream, ct), ct);
            }
         }
         else
         {
            var acceptStreamResult = await session.AcceptStreamAsync(ct);
            if (!acceptStreamResult.Failed) await HandleServerStreamAsync(acceptStreamResult.Success, ct);
         }
      }
      catch (Exception)
      {
         // Connection reset / closed
      }
      finally
      {
         Interlocked.Decrement(ref _serverSessionsActive);
         await session.DisposeAsync();
      }
   }

   private static async Task HandleServerStreamAsync(INetworkStream stream, CancellationToken ct)
   {
      try
      {
         var reader = stream.Transport.Input;
         var writer = stream.Transport.Output;

         while (!ct.IsCancellationRequested)
         {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted) break;

            if (buffer.Length >= 4)
            {
               var content = Encoding.UTF8.GetString(buffer.Slice(0, 4).ToArray());
               if (content == "PING")
               {
                  Interlocked.Increment(ref _serverPingsReceived);

                  // Send PONG
                  var memory = writer.GetMemory(4);
                  "PONG"u8.ToArray().CopyTo(memory.Span);
                  writer.Advance(4);
                  await writer.FlushAsync(ct);
               }

               reader.AdvanceTo(buffer.GetPosition(4));
            }
            else
            {
               reader.AdvanceTo(buffer.Start, buffer.End);
            }

            if (result.IsCompleted || result.IsCanceled) break;
         }
      }
      catch (Exception)
      {
         // Closed abruptly or stream aborted
      }
      finally
      {
         await stream.DisposeAsync();
      }
   }

   private static async Task RunClientScenarioAsync(
      ITestTransportFactory factory,
      EndPoint serverEndPoint,
      ClientScenario scenario,
      CancellationToken ct)
   {
      Interlocked.Increment(ref _clientAttempts);

      if (scenario == ClientScenario.ConnectionErrorSimulated)
      {
         // Try to connect to a closed port
         var closedPortEndPoint = new IPEndPoint(IPAddress.Loopback, 59999);
         var badClient = factory.CreateClient();

         try
         {
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
            var connectResult = await badClient.ConnectAsync(closedPortEndPoint, linkedCts.Token);

            if (connectResult.Failed)
            {
               Interlocked.Increment(ref _clientConnectFailuresExpected);
            }
            else
            {
               // If successfully connected for some reason, dispose and disconnect
               await connectResult.Success.DisposeAsync();
               await badClient.DisconnectAsync(linkedCts.Token);
            }
         }
         catch
         {
            Interlocked.Increment(ref _clientConnectFailuresExpected);
         }
         finally
         {
            await badClient.DisposeAsync();
         }

         return;
      }

      var client = factory.CreateClient();
      try
      {
         using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
         using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
         var connectResult = await client.ConnectAsync(serverEndPoint, linkedCts.Token);

         if (connectResult.Failed)
         {
            Interlocked.Increment(ref _clientConnectFailuresUnexpected);
            return;
         }

         Interlocked.Increment(ref _clientConnectSuccesses);
         var session = connectResult.Success;

         if (scenario == ClientScenario.ImmediateGracefulDisconnect)
         {
            await client.DisconnectAsync(ct);
            Interlocked.Increment(ref _clientGracefulDisconnects);
            return;
         }

         if (scenario == ClientScenario.ImmediateAbruptDisconnect)
         {
            await client.DisposeAsync();
            Interlocked.Increment(ref _clientAbruptDisconnects);
            return;
         }

         // Open stream
         var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
         if (streamResult.Failed)
         {
            Interlocked.Increment(ref _clientStreamErrors);
            await client.DisconnectAsync(ct);
            return;
         }

         var stream = streamResult.Success;
         var reader = stream.Transport.Input;
         var writer = stream.Transport.Output;

         // Write PING
         var memory = writer.GetMemory(4);
         "PING"u8.ToArray().CopyTo(memory.Span);
         writer.Advance(4);
         await writer.FlushAsync(ct);

         // Read PONG
         using var readTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
         using var readLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, readTimeout.Token);
         var readResult = await reader.ReadAsync(readLinkedCts.Token);
         var buffer = readResult.Buffer;

         if (buffer.Length >= 4)
         {
            var content = Encoding.UTF8.GetString(buffer.Slice(0, 4).ToArray());
            if (content == "PONG") Interlocked.Increment(ref _clientPongsReceived);
            reader.AdvanceTo(buffer.GetPosition(4));
         }
         else
         {
            reader.AdvanceTo(buffer.Start, buffer.End);
         }

         await stream.DisposeAsync();

         if (scenario == ClientScenario.PingPongGraceful)
         {
            await client.DisconnectAsync(ct);
            Interlocked.Increment(ref _clientGracefulDisconnects);
         }
         else if (scenario == ClientScenario.AbruptDisconnectAfterPing)
         {
            await client.DisposeAsync();
            Interlocked.Increment(ref _clientAbruptDisconnects);
         }
      }
      catch (Exception)
      {
         Interlocked.Increment(ref _clientErrors);
      }
      finally
      {
         await client.DisposeAsync();
      }
   }
}

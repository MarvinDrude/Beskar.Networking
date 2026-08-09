using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Networking.RealWorldWavesBenchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      string transportName = args.Length > 0 ? args[0].ToUpperInvariant() : "QUIC";
      int waveCount = args.Length > 1 && int.TryParse(args[1], out var w) ? w : 20;
      int usersPerWave = args.Length > 2 && int.TryParse(args[2], out var users) ? users : 50;
      int messagesPerUser = args.Length > 3 && int.TryParse(args[3], out var m) ? m : 10;
      int waveDelayMs = args.Length > 4 && int.TryParse(args[4], out var d) ? d : 1500;

      // Console.Clear();
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("╔═══════════════════════════════════════════════════════════════════════════════════════╗");
      Console.WriteLine("║                                                                                       ║");
      Console.WriteLine("║                     BESKAR REAL-WORLD USER WAVES BENCHMARK                            ║");
      Console.WriteLine("║     Simulates realistic user waves (50 users/wave, 10 msgs, disconnect & repeat)    ║");
      Console.WriteLine("║                                                                                       ║");
      Console.WriteLine("╚═══════════════════════════════════════════════════════════════════════════════════════╝");
      Console.ResetColor();
      Console.WriteLine();

      Console.WriteLine($"Transport Selected      : {transportName}");
      Console.WriteLine($"Total Waves             : {waveCount}");
      Console.WriteLine($"Users Per Wave          : {usersPerWave}");
      Console.WriteLine($"Messages Per User Stream: {messagesPerUser}");
      Console.WriteLine($"Delay Between Waves     : {waveDelayMs} ms");
      Console.WriteLine($"Total Distinct Users    : {waveCount * usersPerWave}");
      Console.WriteLine();

      int port = 9550;
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      X509Certificate2? cert = null;
      INetworkListener? listener = null;

      if (transportName == "QUIC")
      {
         if (!QuicListener.IsSupported)
         {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("QUIC is not supported on this platform.");
            Console.ResetColor();
            return;
         }

         cert = CertificateHelper.GenerateSelfSignedCertificate();
         var quicOptions = new QuicTransportOptions
         {
            IdleTimeout = TimeSpan.FromSeconds(30),
            HandshakeTimeout = TimeSpan.FromSeconds(5),
            MaxPendingConnections = 128,
            MaxInboundBidirectionalStreams = 100,
            SslServerOptions = new SslServerAuthenticationOptions { ServerCertificate = cert },
            SslClientOptions = new SslClientAuthenticationOptions
            {
               TargetHost = "localhost",
               RemoteCertificateValidationCallback = (_, _, _, _) => true
            }
         };
         listener = new QuicNetworkListener(endPoint, quicOptions);
      }
      else if (transportName == "TCP")
      {
         listener = new TcpNetworkListener(endPoint, new TcpTransportOptions());
      }
      else if (transportName == "WS")
      {
         listener = new WsNetworkListener(endPoint, new WsTransportOptions());
      }
      else
      {
         Console.WriteLine("Unknown transport. Choose QUIC, TCP, or WS.");
         return;
      }

      var bindResult = await listener.BindAsync();
      if (bindResult.Failed)
      {
         Console.WriteLine($"Failed to bind listener: {bindResult.Error.Message}");
         return;
      }

      using var serverCts = new CancellationTokenSource();
      var serverTask = Task.Run(() => ServerAcceptLoopAsync(listener, serverCts.Token));

      // Measure baseline memory before waves start
      GC.Collect();
      GC.WaitForPendingFinalizers();
      GC.Collect();

      var initialMemoryMB = Process.GetCurrentProcess().PrivateMemorySize64 / 1024.0 / 1024.0;
      var initialGcMB = GC.GetTotalMemory(true) / 1024.0 / 1024.0;

      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine($"Baseline Memory Before Waves: {initialMemoryMB:F1} MB (GC Managed: {initialGcMB:F1} MB)");
      Console.ResetColor();
      Console.WriteLine();

      Console.WriteLine("┌──────────┬─────────────────┬────────────────────┬─────────────────────┬──────────────────┬─────────────────┐");
      Console.WriteLine("│ Wave #   │ Total Users     │ Active Connections │ Process Memory (MB) │ GC Managed (MB)  │ Memory Delta    │");
      Console.WriteLine("├──────────┼─────────────────┼────────────────────┼─────────────────────┼──────────────────┼─────────────────┤");

      long totalUsersCount = 0;

      for (int wave = 1; wave <= waveCount; wave++)
      {
         var userTasks = new List<Task>();
         for (int userIdx = 0; userIdx < usersPerWave; userIdx++)
         {
            int userId = (wave - 1) * usersPerWave + userIdx + 1;
            userTasks.Add(SimulateSingleUserAsync(endPoint, transportName, cert, userId, messagesPerUser));
         }

         await Task.WhenAll(userTasks);
         totalUsersCount += usersPerWave;

         // Allow short cooldown for native msquic / socket draining timers
         await Task.Delay(waveDelayMs);

         GC.Collect(2, GCCollectionMode.Forced, true, true);

         var currentProcessMB = Process.GetCurrentProcess().PrivateMemorySize64 / 1024.0 / 1024.0;
         var currentGcMB = GC.GetTotalMemory(false) / 1024.0 / 1024.0;
         var deltaMB = currentProcessMB - initialMemoryMB;

         string deltaFormatted = deltaMB >= 0 ? $"+{deltaMB:F1} MB" : $"{deltaMB:F1} MB";

         Console.WriteLine($"│ Wave {wave:D2}/{waveCount:D2} │ {totalUsersCount,15:N0} │ {0,18:N0} │ {currentProcessMB,19:F1} │ {currentGcMB,16:F1} │ {deltaFormatted,15} │");
      }

      Console.WriteLine("└──────────┴─────────────────┴────────────────────┴─────────────────────┴──────────────────┴─────────────────┘");
      Console.WriteLine();

      serverCts.Cancel();
      try { await serverTask; } catch { /* Ignored */ }
      await listener.UnbindAsync();
      cert?.Dispose();

      GC.Collect();
      GC.WaitForPendingFinalizers();
      GC.Collect();

      var finalMemoryMB = Process.GetCurrentProcess().PrivateMemorySize64 / 1024.0 / 1024.0;

      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"Final Process Memory After {totalUsersCount:N0} Total Users: {finalMemoryMB:F1} MB (Peak Delta: +{(finalMemoryMB - initialMemoryMB):F1} MB)");
      Console.ResetColor();
   }

   private static async Task ServerAcceptLoopAsync(INetworkListener listener, CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         var sessionResult = await listener.AcceptSessionAsync(token);
         if (sessionResult.Failed || sessionResult.Success is null) break;

         var session = sessionResult.Success;
         _ = Task.Run(() => HandleServerSessionAsync(session, token), token);
      }
   }

   private static async Task HandleServerSessionAsync(INetworkSession session, CancellationToken token)
   {
      await using (session)
      {
         while (!token.IsCancellationRequested && !session.SessionClosedToken.IsCancellationRequested)
         {
            var streamResult = await session.AcceptStreamAsync(token);
            if (streamResult.Failed || streamResult.Success is null) break;

            var stream = streamResult.Success;
            _ = Task.Run(() => EchoStreamPayloadAsync(stream, token), token);
         }
      }
   }

   private static async Task EchoStreamPayloadAsync(INetworkStream stream, CancellationToken token)
   {
      await using (stream)
      {
         var reader = stream.Transport.Input;
         var writer = stream.Transport.Output;

         while (!token.IsCancellationRequested)
         {
            var result = await reader.ReadAsync(token);
            if (result.IsCanceled || result.Buffer.IsEmpty && result.IsCompleted) break;

            foreach (var segment in result.Buffer)
            {
               await writer.WriteAsync(segment, token);
            }
            await writer.FlushAsync(token);

            reader.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted) break;
         }
      }
   }

   private static async Task SimulateSingleUserAsync(
      EndPoint endPoint, string transportName, X509Certificate2? cert, int userId, int messageCount)
   {
      INetworkClient? client = null;
      try
      {
         if (transportName == "QUIC")
         {
            var quicOptions = new QuicTransportOptions
            {
               IdleTimeout = TimeSpan.FromSeconds(30),
               HandshakeTimeout = TimeSpan.FromSeconds(5),
               SslClientOptions = new SslClientAuthenticationOptions
               {
                  TargetHost = "localhost",
                  RemoteCertificateValidationCallback = (_, _, _, _) => true
               }
            };
            client = new QuicNetworkClient(quicOptions);
         }
         else if (transportName == "TCP")
         {
            client = new TcpNetworkClient(new TcpTransportOptions());
         }
         else if (transportName == "WS")
         {
            client = new WsNetworkClient(new WsTransportOptions());
         }

         if (client is null) return;

         var connectResult = await client.ConnectAsync(endPoint);
         if (connectResult.Failed || connectResult.Success is null) return;

         var session = connectResult.Success;
         var streamResult = await session.OpenStreamAsync();
         if (streamResult.Failed || streamResult.Success is null) return;

         var stream = streamResult.Success;
         await using (stream)
         {
            var payload = "User Message Sample Data Payload "u8.ToArray();

            for (int m = 0; m < messageCount; m++)
            {
               await stream.Transport.Output.WriteAsync(payload);
               await stream.Transport.Output.FlushAsync();

               var readResult = await stream.Transport.Input.ReadAsync();
               if (readResult.Buffer.IsEmpty && readResult.IsCompleted) break;

               stream.Transport.Input.AdvanceTo(readResult.Buffer.End);
            }
         }

         await client.DisconnectAsync();
      }
      catch
      {
         // Ignored
      }
      finally
      {
         if (client is not null)
         {
            await client.DisposeAsync();
         }
      }
   }
}

internal static class CertificateHelper
{
   public static X509Certificate2 GenerateSelfSignedCertificate()
   {
      using var rsa = System.Security.Cryptography.RSA.Create(2048);
      var req = new System.Security.Cryptography.X509Certificates.CertificateRequest(
         "CN=localhost", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
      var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
      return X509CertificateLoader.LoadPkcs12(cert.Export(X509ContentType.Pfx), null);
   }
}

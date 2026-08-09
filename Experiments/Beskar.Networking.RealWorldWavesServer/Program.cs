using System.Diagnostics;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Networking.RealWorldWavesServer;

public static class Program
{
   private static long _activeSessions;
   private static long _totalAccepted;
   private static long _totalStreamsProcessed;

   public static async Task Main(string[] args)
   {
      string transportName = args.Length > 0 ? args[0].ToUpperInvariant() : "QUIC";
      int port = args.Length > 1 && int.TryParse(args[1], out var p) ? p : 9550;

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("╔═══════════════════════════════════════════════════════════════════════════════════════╗");
      Console.WriteLine("║                                                                                       ║");
      Console.WriteLine("║                  BESKAR REAL-WORLD USER WAVES SERVER PROCESS                          ║");
      Console.WriteLine("║     Dedicated Server Process: Monitors Server Process Memory & Connection State       ║");
      Console.WriteLine("║                                                                                       ║");
      Console.WriteLine("╚═══════════════════════════════════════════════════════════════════════════════════════╝");
      Console.ResetColor();
      Console.WriteLine();

      Console.WriteLine($"Transport Selected : {transportName}");
      Console.WriteLine($"Server Endpoint    : 127.0.0.1:{port}");
      Console.WriteLine();

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
            MaxPendingConnections = 256,
            MaxInboundBidirectionalStreams = 100,
            SslServerOptions = new SslServerAuthenticationOptions { ServerCertificate = cert }
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

      using var cts = new CancellationTokenSource();

      // Launch background telemetry reporter
      var reporterTask = Task.Run(() => TelemetryReporterLoopAsync(cts.Token));

      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("Server started! Listening for incoming user wave connections...");
      Console.ResetColor();
      Console.WriteLine();

      Console.WriteLine("┌──────────────────────┬─────────────────┬────────────────────┬─────────────────────┬──────────────────┐");
      Console.WriteLine("│ Time                 │ Total Accepted  │ Active Sessions    │ Server Memory (MB)  │ GC Managed (MB)  │");
      Console.WriteLine("├──────────────────────┼─────────────────┼────────────────────┼─────────────────────┼──────────────────┤");

      try
      {
         while (!cts.Token.IsCancellationRequested)
         {
            var sessionResult = await listener.AcceptSessionAsync(cts.Token);
            if (sessionResult.Failed || sessionResult.Success is null) break;

            var session = sessionResult.Success;
            Interlocked.Increment(ref _totalAccepted);
            Interlocked.Increment(ref _activeSessions);

            _ = Task.Run(() => HandleSessionAsync(session, cts.Token), cts.Token);
         }
      }
      catch (OperationCanceledException)
      {
         // Normal shutdown
      }
      finally
      {
         await listener.UnbindAsync();
         cert?.Dispose();
      }
   }

   private static async Task HandleSessionAsync(INetworkSession session, CancellationToken token)
   {
      try
      {
         await using (session)
         {
            while (!token.IsCancellationRequested && !session.SessionClosedToken.IsCancellationRequested)
            {
               var streamResult = await session.AcceptStreamAsync(token);
               if (streamResult.Failed || streamResult.Success is null) break;

               var stream = streamResult.Success;
               Interlocked.Increment(ref _totalStreamsProcessed);
               _ = Task.Run(() => EchoStreamAsync(stream, token), token);
            }
         }
      }
      finally
      {
         Interlocked.Decrement(ref _activeSessions);
      }
   }

   private static async Task EchoStreamAsync(INetworkStream stream, CancellationToken token)
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

   private static async Task TelemetryReporterLoopAsync(CancellationToken token)
   {
      var initialMem = Process.GetCurrentProcess().PrivateMemorySize64 / 1024.0 / 1024.0;

      while (!token.IsCancellationRequested)
      {
         await Task.Delay(2000, token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

         GC.Collect(2, GCCollectionMode.Forced, true, true);

         var processMB = Process.GetCurrentProcess().PrivateMemorySize64 / 1024.0 / 1024.0;
         var gcMB = GC.GetTotalMemory(false) / 1024.0 / 1024.0;
         var now = DateTime.Now.ToString("HH:mm:ss");

         var accepted = Interlocked.Read(ref _totalAccepted);
         var active = Interlocked.Read(ref _activeSessions);

         Console.WriteLine($"│ {now,-20} │ {accepted,15:N0} │ {active,18:N0} │ {processMB,19:F1} │ {gcMB,16:F1} │");
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

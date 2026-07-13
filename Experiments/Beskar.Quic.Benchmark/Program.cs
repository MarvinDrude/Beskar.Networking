using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Benchmarks.Common;
using Beskar.Networking.Transports.Quic;

namespace Beskar.Quic.Benchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      // Check if QUIC is supported on this machine/OS
      if (!QuicConnection.IsSupported)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine("QUIC transport is not supported on this host platform.");
         Console.ResetColor();
         return;
      }

      // ==========================================
      // DEFAULT BENCHMARK CONFIGURATION
      // ==========================================
      var clientCount = 10;
      var payloadSize = 512;
      var durationSeconds = 10;
      var serverPort = 9003;
      // ==========================================

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("                  BESKAR QUIC BENCHMARK CONFIGURATION             ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      serverPort = PromptInt("Server port", serverPort);
      Console.WriteLine();

      var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
      var config = new BenchmarkConfig(clientCount, payloadSize, durationSeconds, endPoint);

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };

      var options = new QuicTransportOptions
      {
         SslClientOptions = clientSslOptions
      };

      var listener = new QuicNetworkListener(endPoint, options);

      await GenericThroughputBenchmarkRunner.RunAsync(
         listener,
         () => new QuicNetworkClient(options),
         config,
         "QUIC"
      );
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

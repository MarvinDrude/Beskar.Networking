using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Beskar.Networking.Benchmarks.Common;
using Beskar.Networking.Transports.Tcp;

namespace Beskar.Tcp.Benchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      // ==========================================
      // DEFAULT BENCHMARK CONFIGURATION
      // ==========================================
      var clientCount = 20;
      var payloadSize = 512;
      var durationSeconds = 10;
      var serverPort = 9001;
      var useSsl = false;
      // ==========================================

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("                  BESKAR TCP BENCHMARK CONFIGURATION              ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      serverPort = PromptInt("Server port", serverPort);
      useSsl = PromptBool("Use SSL/TLS", useSsl);
      Console.WriteLine();

      var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
      var config = new BenchmarkConfig(clientCount, payloadSize, durationSeconds, endPoint);

      var options = new TcpTransportOptions();
      X509Certificate2? certificate = null;

      if (useSsl)
      {
         certificate = CertificateHelper.GenerateSelfSignedCertificate();
         options.UseSsl = true;
         options.SslServerOptions = new SslServerAuthenticationOptions
         {
            ServerCertificate = certificate,
            ClientCertificateRequired = false
         };
         options.SslClientOptions = new SslClientAuthenticationOptions
         {
            TargetHost = "localhost",
            RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
         };
      }

      try
      {
         var listener = new TcpNetworkListener(endPoint, options);
         await GenericThroughputBenchmarkRunner.RunAsync(
            listener,
            () => new TcpNetworkClient(options),
            config,
            useSsl ? "TCP (SSL/TLS)" : "TCP"
         );
      }
      finally
      {
         certificate?.Dispose();
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

   private static bool PromptBool(string prompt, bool defaultValue)
   {
      Console.Write($"{prompt} (y/n) [default: {(defaultValue ? "y" : "n")}]: ");
      var input = Console.ReadLine();
      if (string.IsNullOrWhiteSpace(input)) return defaultValue;

      var normalized = input.Trim().ToLowerInvariant();
      if (normalized == "y" || normalized == "yes" || normalized == "true" || normalized == "1") return true;
      if (normalized == "n" || normalized == "no" || normalized == "false" || normalized == "0") return false;

      return defaultValue;
   }
}

using System.Net;
using Beskar.Networking.Benchmarks.Common;
using Beskar.Networking.Transports.Udp;

namespace Beskar.Udp.Benchmark;

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
      var serverPort = 9002;
      // ==========================================

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("                  BESKAR UDP BENCHMARK CONFIGURATION              ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      serverPort = PromptInt("Server port", serverPort);
      Console.WriteLine();

      var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
      var config = new BenchmarkConfig(clientCount, payloadSize, durationSeconds, endPoint);

      var options = new UdpTransportOptions();

      var listener = new UdpNetworkListener(endPoint, options);
      await GenericThroughputBenchmarkRunner.RunAsync(
         listener,
         () => new UdpNetworkClient(options),
         config,
         "UDP"
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

using System.Net;
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
      var payloadSize = 1024;
      var durationSeconds = 10;
      var serverPort = 9001;
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
      Console.WriteLine();

      var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
      var config = new BenchmarkConfig(clientCount, payloadSize, durationSeconds, endPoint);

      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(endPoint, options);

      await GenericThroughputBenchmarkRunner.RunAsync(
         listener,
         () => new TcpNetworkClient(options),
         config,
         "TCP"
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

using Beskar.Networking.Benchmarks.Common;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Memory.Benchmark;

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
      var channelName = "benchmark-channel";
      // ==========================================

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("                BESKAR MEMORY BENCHMARK CONFIGURATION             ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      channelName = PromptString("In-memory channel name", channelName);
      Console.WriteLine();

      var endPoint = new MemoryEndPoint(channelName);
      var config = new BenchmarkConfig(clientCount, payloadSize, durationSeconds, endPoint);

      var options = new MemoryTransportOptions();

      var listener = new MemoryNetworkListener(endPoint, options);
      await GenericThroughputBenchmarkRunner.RunAsync(
         listener,
         () => new MemoryNetworkClient(options),
         config,
         "Memory"
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

   private static string PromptString(string prompt, string defaultValue)
   {
      Console.Write($"{prompt} [default: {defaultValue}]: ");
      var input = Console.ReadLine();
      if (string.IsNullOrWhiteSpace(input)) return defaultValue;
      return input.Trim();
   }
}

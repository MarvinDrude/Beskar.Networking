using Beskar.Networking.Benchmarks.Common;
using Beskar.Networking.Transports.NamedPipes;

namespace Beskar.NamedPipes.Benchmark;

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
      // ==========================================

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("            BESKAR NAMED PIPES BENCHMARK CONFIGURATION            ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      Console.WriteLine();

      var pipeName = $"beskar-benchmark-{Guid.NewGuid():N}";
      var endPoint = new NamedPipeEndPoint(pipeName);
      var config = new BenchmarkConfig(clientCount, payloadSize, durationSeconds, endPoint);

      var options = new NamedPipeTransportOptions();

      var listener = new NamedPipeNetworkListener(endPoint, options);
      await GenericThroughputBenchmarkRunner.RunAsync(
         listener,
         () => new NamedPipeNetworkClient(options),
         config,
         "NamedPipe"
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

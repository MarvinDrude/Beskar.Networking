using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace Beskar.Networking.Benchmarks.Common;

[SimpleJob(RuntimeMoniker.HostProcess, 1, 2)]
[MemoryDiagnoser]
public class ValueTaskBenchmarks
{
   private const int Mask = 0x3FF;
   private bool[] _cacheHitPattern = null!;
   private int _index;

   [Params(0, 25, 50, 75, 100)] public int SyncPercentage { get; set; }

   [GlobalSetup]
   public void Setup()
   {
      _cacheHitPattern = new bool[1024];
      for (var i = 0; i < 1024; i++) _cacheHitPattern[i] = i % 100 < SyncPercentage;
   }

   [Benchmark]
   public async ValueTask<int> StandardAsyncValueTask()
   {
      var isHit = _cacheHitPattern[_index++ & Mask];
      return await GetValueStandardAsync(isHit);
   }

   [Benchmark]
   public ValueTask<int> OptimizedValueTask()
   {
      var isHit = _cacheHitPattern[_index++ & Mask];

      if (isHit) return ValueTask.FromResult(42);

      return GetValueOptimizedAsync();

      async ValueTask<int> GetValueOptimizedAsync()
      {
         await Task.Yield();
         return 42;
      }
   }

   [MethodImpl(MethodImplOptions.NoInlining)]
   private async ValueTask<int> GetValueStandardAsync(bool isHit)
   {
      if (isHit) return 42;

      await Task.Yield();
      return 42;
   }
}

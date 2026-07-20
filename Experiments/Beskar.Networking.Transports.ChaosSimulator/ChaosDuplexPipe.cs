using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Beskar.Networking.Transports.ChaosSimulator;

public sealed class ChaosDuplexPipe : IDuplexPipe, IAsyncDisposable
{
   private readonly IDuplexPipe _inner;
   private readonly ChaosOptions _options;

   private readonly Pipe _readPipe;
   private readonly Pipe _writePipe;

   private readonly CancellationTokenSource _cts = new();

   private readonly Task _readPumpTask;
   private readonly Task _writePumpTask;

   private int _disposed;

   public PipeReader Input => _readPipe.Reader;
   public PipeWriter Output => _writePipe.Writer;

   public ChaosDuplexPipe(IDuplexPipe inner, ChaosOptions options)
   {
      _inner = inner;
      _options = options;

      _readPipe = new Pipe();
      _writePipe = new Pipe();

      _readPumpTask = Task.Run(RunReadPumpAsync);
      _writePumpTask = Task.Run(RunWritePumpAsync);
   }

   private async Task RunReadPumpAsync()
   {
      try
      {
         while (!_cts.Token.IsCancellationRequested)
         {
            var result = await _inner.Input.ReadAsync(_cts.Token);
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
               await _readPipe.Writer.CompleteAsync();
               await _readPipe.Reader.CompleteAsync();
               break;
            }

            var data = buffer.ToArray();
            _inner.Input.AdvanceTo(buffer.End);

            if (data.Length > 0)
            {
               var shouldDrop = Random.Shared.NextDouble() < _options.PacketDropRate;
               if (!shouldDrop)
               {
                  if (Random.Shared.NextDouble() < _options.DataCorruptionRate)
                  {
                     CorruptData(data);
                  }

                  if (_options.ReadLatencyRate > 0 && Random.Shared.NextDouble() < _options.ReadLatencyRate)
                  {
                     var latencyMs = Random.Shared.Next(
                        (int)_options.MinReadLatency.TotalMilliseconds,
                        (int)_options.MaxReadLatency.TotalMilliseconds + 1
                     );
                     if (latencyMs > 0)
                     {
                        await Task.Delay(latencyMs, _cts.Token);
                     }
                  }

                  if (_options.MaxReadBytesPerSecond.HasValue && _options.MaxReadBytesPerSecond.Value > 0)
                  {
                     await ApplyThrottling(data.Length, _options.MaxReadBytesPerSecond.Value);
                  }

                  await _readPipe.Writer.WriteAsync(data, _cts.Token);
               }
            }

            if (result.IsCompleted)
            {
               await _readPipe.Writer.CompleteAsync();
               await _readPipe.Reader.CompleteAsync();
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
         await _readPipe.Writer.CompleteAsync();
         await _readPipe.Reader.CompleteAsync();
      }
      catch (Exception ex)
      {
         await _readPipe.Writer.CompleteAsync(ex);
         await _readPipe.Reader.CompleteAsync(ex);
      }
   }

   private async Task RunWritePumpAsync()
   {
      try
      {
         while (!_cts.Token.IsCancellationRequested)
         {
            var result = await _writePipe.Reader.ReadAsync(_cts.Token);
            var buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
               await _inner.Output.CompleteAsync();
               await _writePipe.Reader.CompleteAsync();
               await _writePipe.Writer.CompleteAsync();
               break;
            }

            var data = buffer.ToArray();
            _writePipe.Reader.AdvanceTo(buffer.End);

            if (data.Length > 0)
            {
               var shouldDrop = Random.Shared.NextDouble() < _options.PacketDropRate;
               if (!shouldDrop)
               {
                  if (Random.Shared.NextDouble() < _options.DataCorruptionRate)
                  {
                     CorruptData(data);
                  }

                  if (_options.WriteLatencyRate > 0 && Random.Shared.NextDouble() < _options.WriteLatencyRate)
                  {
                     var latencyMs = Random.Shared.Next(
                        (int)_options.MinWriteLatency.TotalMilliseconds,
                        (int)_options.MaxWriteLatency.TotalMilliseconds + 1
                     );
                     if (latencyMs > 0)
                     {
                        await Task.Delay(latencyMs, _cts.Token);
                     }
                  }

                  if (_options.MaxWriteBytesPerSecond.HasValue && _options.MaxWriteBytesPerSecond.Value > 0)
                  {
                     await ApplyThrottling(data.Length, _options.MaxWriteBytesPerSecond.Value);
                  }

                  await _inner.Output.WriteAsync(data, _cts.Token);
               }
            }

            if (result.IsCompleted)
            {
               await _inner.Output.CompleteAsync();
               await _writePipe.Reader.CompleteAsync();
               await _writePipe.Writer.CompleteAsync();
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
         await _inner.Output.CompleteAsync();
         await _writePipe.Reader.CompleteAsync();
         await _writePipe.Writer.CompleteAsync();
      }
      catch (Exception ex)
      {
         await _inner.Output.CompleteAsync(ex);
         await _writePipe.Reader.CompleteAsync(ex);
         await _writePipe.Writer.CompleteAsync(ex);
      }
   }

   private static void CorruptData(byte[] data)
   {
      if (data.Length == 0) return;
      var index = Random.Shared.Next(data.Length);
      // Flip all bits in the chosen byte to guarantee a mismatching checksum
      data[index] = (byte)(data[index] ^ 0xFF);
   }

   private async Task ApplyThrottling(int byteCount, long limitPerSecond)
   {
      var delayMs = (double)byteCount / limitPerSecond * 1000.0;
      if (delayMs >= 1)
      {
         await Task.Delay((int)delayMs, _cts.Token);
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }
      _cts.Dispose();

      try
      {
         await Task.WhenAll(_readPumpTask, _writePumpTask);
      }
      catch
      {
         // Ignored
      }

      await _writePipe.Reader.CompleteAsync();
      await _writePipe.Writer.CompleteAsync();
      await _readPipe.Reader.CompleteAsync();
      await _readPipe.Writer.CompleteAsync();
   }
}

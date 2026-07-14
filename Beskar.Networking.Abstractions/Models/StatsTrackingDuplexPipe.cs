using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// A decorator for <see cref="IDuplexPipe"/> that automatically increments stream stats on read and write.
/// </summary>
public sealed class StatsTrackingDuplexPipe : IDuplexPipe
{
   public PipeReader Input { get; }
   public PipeWriter Output { get; }

   public StatsTrackingDuplexPipe(IDuplexPipe inner, INetworkStream stream)
   {
      ArgumentNullException.ThrowIfNull(inner);
      ArgumentNullException.ThrowIfNull(stream);

      Input = new StatsTrackingPipeReader(inner.Input, stream);
      Output = new StatsTrackingPipeWriter(inner.Output, stream);
   }

   private sealed class StatsTrackingPipeReader(PipeReader inner, INetworkStream stream) : PipeReader
   {
      private readonly PipeReader _inner = inner;
      private readonly INetworkStream _stream = stream;

      private ReadOnlySequence<byte> _lastBuffer;

      public override void AdvanceTo(SequencePosition consumed)
      {
         AdvanceTo(consumed, consumed);
      }

      public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
      {
         if (!_lastBuffer.IsEmpty)
         {
            var bytesConsumed = _lastBuffer.Slice(_lastBuffer.Start, consumed).Length;

            if (bytesConsumed > 0)
            {
               var stats = _stream.Stats;
               stats.BytesReceived += bytesConsumed;
               stats.LastReceivedTimestamp = DateTimeOffset.UtcNow;

               _stream.Stats = stats;
               _lastBuffer = _lastBuffer.Slice(consumed);
            }
         }

         _inner.AdvanceTo(consumed, examined);
      }

      public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
      {
         var result = await _inner.ReadAsync(cancellationToken);
         _lastBuffer = result.Buffer;

         return result;
      }

      public override bool TryRead(out ReadResult result)
      {
         if (_inner.TryRead(out result))
         {
            _lastBuffer = result.Buffer;
            return true;
         }

         return false;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void CancelPendingRead() => _inner.CancelPendingRead();

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void Complete(Exception? exception = null) => _inner.Complete(exception);
   }

   private sealed class StatsTrackingPipeWriter(PipeWriter inner, INetworkStream stream) : PipeWriter
   {
      private readonly PipeWriter _inner = inner;
      private readonly INetworkStream _stream = stream;

      public override void Advance(int bytes)
      {
         var stats = _stream.Stats;
         stats.BytesSent += bytes;
         stats.LastSentTimestamp = DateTimeOffset.UtcNow;
         _stream.Stats = stats;

         _inner.Advance(bytes);
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override Memory<byte> GetMemory(int sizeHint = 0)
         => _inner.GetMemory(sizeHint);

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override Span<byte> GetSpan(int sizeHint = 0)
         => _inner.GetSpan(sizeHint);

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
         => _inner.FlushAsync(cancellationToken);

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void CancelPendingFlush()
         => _inner.CancelPendingFlush();

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void Complete(Exception? exception = null)
         => _inner.Complete(exception);
   }
}

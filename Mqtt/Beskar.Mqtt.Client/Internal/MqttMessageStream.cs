using System.Threading.Channels;

namespace Beskar.Mqtt.Client.Internal;

/// <summary>
/// An <see cref="IAsyncEnumerable{T}"/> implementation backed by a <see cref="Channel{T}"/>
/// that automatically unregisters its subscription and cleans up resources upon disposal.
/// </summary>
/// <typeparam name="T">The decoded message item type.</typeparam>
internal sealed class MqttMessageStream<T> : IAsyncEnumerable<T>, IAsyncDisposable
{
   private readonly Channel<T> _channel;
   private readonly Func<ValueTask> _onDispose;

   private readonly CancellationToken _streamCt;
   private readonly CancellationTokenRegistration _ctr;
   private int _disposed;

   public MqttMessageStream(
      Channel<T> channel,
      Func<ValueTask> onDispose,
      CancellationToken ct = default)
   {
      _channel = channel ?? throw new ArgumentNullException(nameof(channel));
      _onDispose = onDispose ?? throw new ArgumentNullException(nameof(onDispose));
      _streamCt = ct;

      if (ct.CanBeCanceled)
      {
         _ctr = ct.Register(static s =>
         {
            var stream = (MqttMessageStream<T>)s!;
            _ = stream.DisposeAsync();
         }, this);
      }
   }

   public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
   {
      return new Enumerator(this, cancellationToken);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 0)
      {
         _ctr.Dispose();
         _channel.Writer.TryComplete();
         await _onDispose().ConfigureAwait(false);
      }
   }

   private sealed class Enumerator : IAsyncEnumerator<T>
   {
      private readonly MqttMessageStream<T> _stream;
      private readonly CancellationTokenSource? _linkedCts;
      private readonly CancellationToken _effectiveToken;
      private T? _current;

      public Enumerator(MqttMessageStream<T> stream, CancellationToken cancellationToken)
      {
         _stream = stream;

         if (cancellationToken.CanBeCanceled && stream._streamCt.CanBeCanceled)
         {
            _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stream._streamCt);
            _effectiveToken = _linkedCts.Token;
         }
         else if (cancellationToken.CanBeCanceled)
         {
            _effectiveToken = cancellationToken;
         }
         else
         {
            _effectiveToken = stream._streamCt;
         }
      }

      public T Current => _current!;

      public async ValueTask<bool> MoveNextAsync()
      {
         if (_effectiveToken.IsCancellationRequested)
         {
            return false;
         }

         try
         {
            while (await _stream._channel.Reader.WaitToReadAsync(_effectiveToken).ConfigureAwait(false))
            {
               if (_effectiveToken.IsCancellationRequested)
               {
                  return false;
               }

               if (_stream._channel.Reader.TryRead(out var item))
               {
                  _current = item;
                  return true;
               }
            }
         }
         catch (OperationCanceledException)
         {
            return false;
         }
         catch (ChannelClosedException)
         {
            return false;
         }

         return false;
      }

      public async ValueTask DisposeAsync()
      {
         _linkedCts?.Dispose();
         _current = default;

         await _stream.DisposeAsync().ConfigureAwait(false);
      }
   }
}

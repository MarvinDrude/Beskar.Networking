using System.Threading.Channels;

namespace Beskar.Mqtt.Client.Internal;

/// <summary>
/// An <see cref="IAsyncEnumerable{T}"/> implementation backed by a <see cref="Channel{T}"/>
/// that automatically unregisters its subscription and cleans up resources upon disposal.
/// </summary>
/// <typeparam name="T">The decoded message item type.</typeparam>
internal sealed class MqttMessageStream<T>(
   Channel<T> channel,
   Func<ValueTask> onDispose)
   : IAsyncEnumerable<T>, IAsyncDisposable
{
   private readonly Channel<T> _channel = channel ?? throw new ArgumentNullException(nameof(channel));
   private readonly Func<ValueTask> _onDispose = onDispose ?? throw new ArgumentNullException(nameof(onDispose));
   private int _disposed;

   public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
   {
      return new Enumerator(this, cancellationToken);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 0)
      {
         _channel.Writer.TryComplete();
         await _onDispose().ConfigureAwait(false);
      }
   }

   private sealed class Enumerator(MqttMessageStream<T> stream, CancellationToken cancellationToken)
      : IAsyncEnumerator<T>
   {
      private readonly MqttMessageStream<T> _stream = stream;
      private readonly CancellationToken _cancellationToken = cancellationToken;
      private T? _current;

      public T Current => _current!;

      public async ValueTask<bool> MoveNextAsync()
      {
         if (_cancellationToken.IsCancellationRequested)
         {
            return false;
         }

         try
         {
            while (await _stream._channel.Reader.WaitToReadAsync(_cancellationToken).ConfigureAwait(false))
            {
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

      public ValueTask DisposeAsync()
      {
         _current = default;
         return _stream.DisposeAsync();
      }
   }
}

using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Beskar.Mqtt.Common.Generators;

public sealed class SignalAwaiter<TResponseMessage>(ushort identifier)
   : ISignalAwaiter<TResponseMessage>, IValueTaskSource<TResponseMessage>
{
   public ushort Identifier { get; private set; } = identifier;
   public Type MessageType { get; } = typeof(TResponseMessage);

   /// <summary>
   /// Collision resolution (only for packets not supporting ids)
   /// Effectively just things like PING
   /// </summary>
   public ISignalAwaiter? Next { get; set; }

   // 0 = Pending, 1 = Completed, 2 = Failed/Canceled, 3 = Disposed
   private int _state;

   private CancellationTokenRegistration _cancellationRegistration;
   private ManualResetValueTaskSourceCore<TResponseMessage> _core = new()
   {
      RunContinuationsAsynchronously = true
   };

   private SignalBroker? _broker;

   public ValueTask<TResponseMessage> WaitOneAsync(CancellationToken cancellationToken)
   {
      if (cancellationToken.CanBeCanceled)
      {
         _cancellationRegistration = cancellationToken.Register(
            static state => ((SignalAwaiter<TResponseMessage>)state!).Fail(new TimeoutException()),
            this);
      }

      return new ValueTask<TResponseMessage>(this, _core.Version);
   }

   public bool TryComplete(TResponseMessage message)
   {
      if (Interlocked.CompareExchange(ref _state, 1, 0) == 0)
      {
         _core.SetResult(message);
         _cancellationRegistration.Dispose();

         return true;
      }

      return false;
   }

   public bool TryComplete<TIncoming>(TIncoming message)
   {
      if (typeof(TIncoming) == typeof(TResponseMessage))
      {
         return TryComplete(Unsafe.As<TIncoming, TResponseMessage>(ref message));
      }

      if (message is TResponseMessage responseMessage)
      {
         return TryComplete(responseMessage);
      }

      return false;
   }

   public void Cancel()
   {
      if (Interlocked.CompareExchange(ref _state, 2, 0) == 0)
      {
         _core.SetException(new OperationCanceledException());
         _cancellationRegistration.Dispose();
      }
   }

   public void Fail(Exception exception)
   {
      if (Interlocked.CompareExchange(ref _state, 2, 0) == 0)
      {
         _core.SetException(exception);
         _cancellationRegistration.Dispose();
      }
   }

   public void Reset(ushort newIdentifier, SignalBroker broker)
   {
      Identifier = newIdentifier;
      Next = null;

      _broker = broker;
      _state = 0;

      _cancellationRegistration.Dispose();
      _cancellationRegistration = default;

      _core.Reset();
   }

   public void Dispose()
   {
      var previousState = Interlocked.Exchange(ref _state, 3);
      if (previousState == 3) return;

      var isSafeToPool = false;
      switch (previousState)
      {
         case 1:
            // HAPPY PATH: The broker successfully completed it
            isSafeToPool = true;
            break;
         case 0: // Manual early dispose
         case 2: // Timed out or manually canceled
         {
            if (previousState == 0)
            {
               _core.SetException(new OperationCanceledException());
               _cancellationRegistration.Dispose();
            }

            if (_broker != null)
            {
               isSafeToPool = _broker.TryRemove(Identifier, this);
            }

            break;
         }
      }

      if (!isSafeToPool) return;

      Next = null;
      _broker = null;

      SignalAwaiterPool<TResponseMessage>.Return(this);
   }

   TResponseMessage IValueTaskSource<TResponseMessage>.GetResult(short token)
      => _core.GetResult(token);

   ValueTaskSourceStatus IValueTaskSource<TResponseMessage>.GetStatus(short token)
      => _core.GetStatus(token);

   void IValueTaskSource<TResponseMessage>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
      => _core.OnCompleted(continuation, state, token, flags);
}

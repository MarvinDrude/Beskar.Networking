using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Beskar.Mqtt.Common.Generators;

public sealed class SignalAwaiter<TResponseMessage>(ushort identifier)
   : ISignalAwaiter<TResponseMessage>, IValueTaskSource<TResponseMessage>
{
   public ushort Identifier { get; private set; } = identifier;
   public Type MessageType { get; } = typeof(TResponseMessage);

   public bool IsPending => Volatile.Read(ref _state) == 0;
   public bool IsCompletedSuccessfully => Volatile.Read(ref _state) == 1;
   public bool IsPruned => _isPruned;

   /// <summary>
   /// Collision resolution (only for packets not supporting ids)
   /// Effectively just things like PING
   /// </summary>
   public ISignalAwaiter? Next { get; set; }

   // 0 = Pending, 1 = Completed, 2 = Failed/Canceled, 3 = Disposed
   private int _state;
   private int _isDisposed;
   private volatile bool _isPruned;

   private int _isPooled;
   private int _isResultRetrieved;
   private int _hasContinuation;

   // Bit 1 = Pruned, Bit 2 = Disposed, Bit 4 = ResultRetrieved (or no continuation registered)
   private int _lifecycleState;

   private CancellationTokenRegistration _cancellationRegistration;

   private ManualResetValueTaskSourceCore<TResponseMessage> _core = new()
   {
      RunContinuationsAsynchronously = true
   };

   private volatile SignalBroker? _broker;

   public ValueTask<TResponseMessage> WaitOneAsync(CancellationToken cancellationToken)
   {
      if (cancellationToken.CanBeCanceled)
      {
         _cancellationRegistration = cancellationToken.Register(
            static (state, token) => ((SignalAwaiter<TResponseMessage>)state!).Cancel(token),
            this);
      }

      return new ValueTask<TResponseMessage>(this, _core.Version);
   }

   public bool TryComplete(in TResponseMessage message)
   {
      if (Interlocked.CompareExchange(ref _state, 1, 0) == 0)
      {
         _cancellationRegistration.Dispose();
         _cancellationRegistration = default;

         _core.SetResult(message);

         return true;
      }

      return false;
   }

   public bool TryComplete<TIncoming>(in TIncoming message)
   {
      if (typeof(TIncoming) == typeof(TResponseMessage))
      {
         return TryComplete(Unsafe.As<TIncoming, TResponseMessage>(ref Unsafe.AsRef(in message)));
      }

      if (message is TResponseMessage responseMessage)
      {
         return TryComplete(responseMessage);
      }

      return false;
   }

   public void Cancel() => Cancel(CancellationToken.None);

   public void Cancel(CancellationToken token)
   {
      if (Interlocked.CompareExchange(ref _state, 2, 0) == 0)
      {
         _cancellationRegistration.Dispose();
         _cancellationRegistration = default;

         _core.SetException(new OperationCanceledException(token));
      }
   }

   public void Fail(Exception exception)
   {
      if (Interlocked.CompareExchange(ref _state, 2, 0) == 0)
      {
         _cancellationRegistration.Dispose();
         _cancellationRegistration = default;

         _core.SetException(exception);
      }
   }

   private bool TryPool()
   {
      if (Interlocked.CompareExchange(ref _isPooled, 1, 0) == 0)
      {
         Next = null;
         _broker = null;

         SignalAwaiterPool<TResponseMessage>.Return(this);
         return true;
      }

      return false;
   }

   private void CompleteLifecycleFlag(int mask)
   {
      var prev = Interlocked.Or(ref _lifecycleState, mask);
      if ((prev & 0b111) != 0b111 && ((prev | mask) & 0b111) == 0b111)
      {
         TryPool();
      }
   }

   public void OnPruned()
   {
      _isPruned = true;
      CompleteLifecycleFlag(0b001);
   }

   public void Reset(ushort newIdentifier, SignalBroker broker)
   {
      _cancellationRegistration.Dispose();
      _cancellationRegistration = default;

      _core.Reset();

      Identifier = newIdentifier;
      Next = null;
      _isPruned = false;

      _isPooled = 0;
      _isResultRetrieved = 0;
      _hasContinuation = 0;
      _lifecycleState = 0;
      _isDisposed = 0;

      _broker = broker;
      _state = 0;
   }

   public void Dispose()
   {
      if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) != 0) return;

      var previousState = Interlocked.Exchange(ref _state, 3);
      if (previousState == 3) return;

      _cancellationRegistration.Dispose();
      _cancellationRegistration = default;

      if (previousState == 0)
      {
         _core.SetException(new OperationCanceledException());
      }

      if (!_isPruned)
      {
         _broker?.TryRemove(Identifier, this);
      }

      var mask = Volatile.Read(ref _hasContinuation) == 0 ? 0b110 : 0b010;
      CompleteLifecycleFlag(mask);
   }

   TResponseMessage IValueTaskSource<TResponseMessage>.GetResult(short token)
   {
      try
      {
         return _core.GetResult(token);
      }
      finally
      {
         if (Interlocked.CompareExchange(ref _isResultRetrieved, 1, 0) == 0)
         {
            CompleteLifecycleFlag(0b100);
         }
      }
   }

   ValueTaskSourceStatus IValueTaskSource<TResponseMessage>.GetStatus(short token)
      => _core.GetStatus(token);

   void IValueTaskSource<TResponseMessage>.OnCompleted(
      Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
   {
      Volatile.Write(ref _hasContinuation, 1);
      _core.OnCompleted(continuation, state, token, flags);
   }
}

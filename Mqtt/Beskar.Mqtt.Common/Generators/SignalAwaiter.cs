using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Beskar.Mqtt.Common.Generators;

public sealed class SignalAwaiter<TResponseMessage>(ushort identifier)
   : ISignalAwaiter<TResponseMessage>, IValueTaskSource<TResponseMessage>
{
   public ushort Identifier { get; private set; } = identifier;
   public Type MessageType { get; } = typeof(TResponseMessage);
   public bool IsPending => Volatile.Read(ref _state) == 0;

   /// <summary>
   /// Collision resolution (only for packets not supporting ids)
   /// Effectively just things like PING
   /// </summary>
   public ISignalAwaiter? Next { get; set; }

   // 0 = Pending, 1 = Completed, 2 = Failed/Canceled, 3 = Disposed
   private int _state;
   private volatile bool _isPruned;
   private int _isPooled;

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
            static state => ((SignalAwaiter<TResponseMessage>)state!).Fail(new TimeoutException()),
            this);
      }

      return new ValueTask<TResponseMessage>(this, _core.Version);
   }

   public bool TryComplete(in TResponseMessage message)
   {
      if (Interlocked.CompareExchange(ref _state, 1, 0) == 0)
      {
         _core.SetResult(message);
         _cancellationRegistration.Dispose();

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

   public void OnPruned()
   {
      _isPruned = true;
      if (Volatile.Read(ref _state) == 3)
      {
         TryPool();
      }
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

      _broker = broker;
      _state = 0;
   }

   public void Dispose()
   {
      var previousState = Interlocked.Exchange(ref _state, 3);
      if (previousState == 3) return;

      var isSafeToPool = false;
      if (previousState == 0)
      {
         _core.SetException(new OperationCanceledException());
         _cancellationRegistration.Dispose();
      }

      var broker = _broker;
      if (_isPruned)
      {
         isSafeToPool = true;
      }
      else if (broker is not null)
      {
         isSafeToPool = broker.TryRemove(Identifier, this);
      }

      if (!isSafeToPool) return;
      TryPool();
   }

   TResponseMessage IValueTaskSource<TResponseMessage>.GetResult(short token)
      => _core.GetResult(token);

   ValueTaskSourceStatus IValueTaskSource<TResponseMessage>.GetStatus(short token)
      => _core.GetStatus(token);

   void IValueTaskSource<TResponseMessage>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
      => _core.OnCompleted(continuation, state, token, flags);
}

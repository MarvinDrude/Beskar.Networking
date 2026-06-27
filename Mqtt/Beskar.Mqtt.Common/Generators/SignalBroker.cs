using System.Runtime.CompilerServices;

namespace Beskar.Mqtt.Common.Generators;

public sealed class SignalBroker : IDisposable
{
   /// <summary>
   /// Since we want the fastest lookups and registrations in a thread safe way
   /// and we know exactly the maximum identifier possible, we just allocate the complete
   /// range of possible indexes here. It allows us to completely throw out any locking etc.
   /// </summary>
   private readonly ISignalAwaiter?[] _waiters = new ISignalAwaiter?[65536];
   private int _isDisposed;

   public SignalAwaiter<TResponseMessage> AddAwaitable<TResponseMessage>(ushort identifier)
   {
      ThrowIfDisposed();
      var awaitable = SignalAwaiterPool<TResponseMessage>.Get(identifier, this);

      while (true)
      {
         var currentHead = Volatile.Read(ref _waiters[identifier]);

         // Prune dead nodes at the head of the chain
         if (currentHead is not null && !currentHead.IsPending)
         {
            var next = currentHead.Next;
            if (Interlocked.CompareExchange(ref _waiters[identifier], next, currentHead) == currentHead)
            {
               currentHead.OnPruned();
            }

            continue;
         }

         awaitable.Next = currentHead;
         if (Interlocked.CompareExchange(ref _waiters[identifier], awaitable, currentHead) == currentHead)
         {
            if (Volatile.Read(ref _isDisposed) == 1)
            {
               if (TryRemove(identifier, awaitable))
               {
                  awaitable.Fail(new ObjectDisposedException(nameof(SignalBroker)));
                  awaitable.Dispose();
               }

               throw new ObjectDisposedException(nameof(SignalBroker));
            }

            return awaitable;
         }
      }
   }

   public bool TryDispatch<TResponseMessage>(TResponseMessage message, ushort identifier)
   {
      ArgumentNullException.ThrowIfNull(message);
      ThrowIfDisposed();

      var msgType = message.GetType();

      while (true)
      {
         var currentHead = Volatile.Read(ref _waiters[identifier]);
         if (currentHead is null)
         {
            return false;
         }

         if (!currentHead.IsPending)
         {
            var next = currentHead.Next;
            if (Interlocked.CompareExchange(ref _waiters[identifier], next, currentHead) == currentHead)
            {
               currentHead.OnPruned();
            }

            continue;
         }

         if (currentHead.MessageType == msgType)
         {
            // fast path
            var next = currentHead.Next;
            if (Interlocked.CompareExchange(ref _waiters[identifier], next, currentHead) == currentHead)
            {
               currentHead.OnPruned();
               return currentHead.TryComplete(message);
            }

            // oh no a collision! retry
            continue;
         }

         var current = currentHead.Next;
         while (current is not null)
         {
            if (current.MessageType == msgType)
            {
               return current.TryComplete(message);
            }

            current = current.Next;
         }

         return false; // Message type not found in this ID's chain
      }
   }

   public bool TryRemove<TResponseMessage>(ushort identifier, SignalAwaiter<TResponseMessage> awaiter)
   {
      while (true)
      {
         var currentHead = Volatile.Read(ref _waiters[identifier]);
         if (currentHead is null)
         {
            return false;
         }

         // Prune dead nodes at the head of the chain
         if (!currentHead.IsPending)
         {
            var next = currentHead.Next;
            if (Interlocked.CompareExchange(ref _waiters[identifier], next, currentHead) == currentHead)
            {
               currentHead.OnPruned();
               if (ReferenceEquals(currentHead, awaiter))
               {
                  return true;
               }
            }

            continue;
         }

         return false;
      }
   }

   public void Dispose()
   {
      if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) != 0) return;
      var exception = new ObjectDisposedException(nameof(SignalBroker));

      // Heavy loop over all, but since we just do this in case of a complete disposal
      // of a server or client, it should be fine.
      for (var i = 0; i < _waiters.Length; i++)
      {
         // Atomically extract the entire chain for this index and wipe the slot
         var current = Interlocked.Exchange(ref _waiters[i], null);

         while (current != null)
         {
            current.Fail(exception);
            var next = current.Next;
            current.OnPruned();
            current = next;
         }
      }
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   private void ThrowIfDisposed()
   {
      ObjectDisposedException.ThrowIf(_isDisposed == 1, this);
   }
}

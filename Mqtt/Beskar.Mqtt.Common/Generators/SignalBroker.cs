using System.Linq;
using System.Runtime.CompilerServices;

namespace Beskar.Mqtt.Common.Generators;

public sealed class SignalBroker : IDisposable
{
   private readonly ISignalAwaiter?[] _waiters = new ISignalAwaiter?[65536];
   private readonly Lock[] _locks = [.. Enumerable.Range(0, 1024).Select(_ => new Lock())];

   private int _isDisposed;

   public SignalAwaiter<TResponseMessage> AddAwaitable<TResponseMessage>(ushort identifier)
   {
      ThrowIfDisposed();
      var awaitable = SignalAwaiterPool<TResponseMessage>.Get(identifier, this);

      lock (_locks[identifier % 1024])
      {
         var currentHead = _waiters[identifier];

         // Prune dead nodes at the head of the chain
         while (currentHead is not null && !currentHead.IsPending)
         {
            var next = currentHead.Next;
            _waiters[identifier] = next;

            currentHead.OnPruned();
            currentHead = next;
         }

         awaitable.Next = currentHead;
         _waiters[identifier] = awaitable;

         if (Volatile.Read(ref _isDisposed) == 1)
         {
            if (TryRemoveInternal(identifier, awaitable))
            {
               awaitable.Fail(new ObjectDisposedException(nameof(SignalBroker)));
            }

            awaitable.Dispose();
            throw new ObjectDisposedException(nameof(SignalBroker));
         }

         return awaitable;
      }
   }

   public bool TryDispatch<TResponseMessage>(in TResponseMessage message, ushort identifier)
   {
      if (message is null)
      {
         throw new ArgumentNullException(nameof(message));
      }

      ThrowIfDisposed();
      var msgType = message.GetType();

      lock (_locks[identifier % 1024])
      {
         while (true)
         {
            var currentHead = _waiters[identifier];

            // Prune dead nodes at the head of the chain
            while (currentHead is not null && !currentHead.IsPending)
            {
               var next = currentHead.Next;
               _waiters[identifier] = next;

               currentHead.OnPruned();
               currentHead = next;
            }

            if (currentHead is null)
            {
               return false;
            }

            if (currentHead.MessageType == msgType)
            {
               if (currentHead.TryComplete(in message))
               {
                  var next = currentHead.Next;
                  _waiters[identifier] = next;

                  currentHead.OnPruned();
                  return true;
               }

               continue;
            }

            break;
         }

         var prevHead = _waiters[identifier];
         if (prevHead is null)
         {
            return false;
         }

         var prev = prevHead;
         var current = prev.Next;

         while (current is not null)
         {
            // Prune dead/completed nodes in the chain
            if (!current.IsPending)
            {
               prev.Next = current.Next;
               current.OnPruned();

               current = prev.Next;
               continue;
            }

            if (current.MessageType == msgType)
            {
               if (current.TryComplete(in message))
               {
                  prev.Next = current.Next;
                  current.OnPruned();
                  return true;
               }

               prev.Next = current.Next;
               current.OnPruned();

               current = prev.Next;
               continue;
            }

            prev = current;
            current = current.Next;
         }

         return false;
      }
   }

   public bool TryRemove<TResponseMessage>(ushort identifier, SignalAwaiter<TResponseMessage> awaiter)
   {
      lock (_locks[identifier % 1024])
      {
         if (awaiter.IsPruned)
         {
            return false;
         }

         return TryRemoveInternal(identifier, awaiter);
      }
   }

   private bool TryRemoveInternal<TResponseMessage>(ushort identifier, SignalAwaiter<TResponseMessage> awaiter)
   {
      var currentHead = _waiters[identifier];
      if (currentHead is null)
      {
         return false;
      }

      if (ReferenceEquals(currentHead, awaiter))
      {
         _waiters[identifier] = currentHead.Next;
         currentHead.OnPruned();
         return true;
      }

      var prev = currentHead;
      var current = currentHead.Next;

      while (current is not null)
      {
         if (ReferenceEquals(current, awaiter))
         {
            prev.Next = current.Next;
            current.OnPruned();

            return true;
         }

         prev = current;
         current = current.Next;
      }

      return false;
   }

   public void Reset()
   {
      ThrowIfDisposed();
      var exception = new OperationCanceledException("The broker was reset.");

      for (var i = 0; i < _waiters.Length; i++)
      {
         lock (_locks[i % 1024])
         {
            var current = _waiters[i];
            _waiters[i] = null;

            while (current != null)
            {
               current.Fail(exception);
               var next = current.Next;

               current.OnPruned();
               current = next;
            }
         }
      }
   }

   public void Dispose()
   {
      if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) != 0) return;
      var exception = new ObjectDisposedException(nameof(SignalBroker));

      for (var i = 0; i < _waiters.Length; i++)
      {
         lock (_locks[i % 1024])
         {
            var current = _waiters[i];
            _waiters[i] = null;

            while (current != null)
            {
               current.Fail(exception);
               var next = current.Next;

               current.OnPruned();
               current = next;
            }
         }
      }
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   private void ThrowIfDisposed()
   {
      ObjectDisposedException.ThrowIf(_isDisposed == 1, this);
   }
}

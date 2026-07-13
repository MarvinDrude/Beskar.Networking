namespace Beskar.Networking.Abstractions.Threading;

public sealed class ReadWriteLock(
   LockRecursionPolicy recursionPolicy = LockRecursionPolicy.SupportsRecursion)
   : IDisposable
{
   private readonly ReaderWriterLockSlim _lock = new(recursionPolicy);

   public IDisposable EnterWriteLock(CancellationToken ct = default)
   {
      _lock.EnterWriteLock();
      return new WriteDisposer(_lock);
   }

   public IDisposable EnterReadLock(CancellationToken ct = default)
   {
      _lock.EnterReadLock();
      return new ReadDisposer(_lock);
   }

   public void Dispose()
   {
      _lock.Dispose();
   }

   private readonly struct WriteDisposer(ReaderWriterLockSlim lockSlim) : IDisposable
   {
      public void Dispose()
      {
         lockSlim.ExitWriteLock();
      }
   }

   private readonly struct ReadDisposer(ReaderWriterLockSlim lockSlim) : IDisposable
   {
      public void Dispose()
      {
         lockSlim.ExitReadLock();
      }
   }
}

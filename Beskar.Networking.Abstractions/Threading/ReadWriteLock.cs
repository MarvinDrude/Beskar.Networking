namespace Beskar.Networking.Abstractions.Threading;

public sealed class ReadWriteLock
{
   private readonly ReaderWriterLockSlim _lock = new();

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

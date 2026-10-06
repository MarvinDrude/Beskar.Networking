namespace Beskar.Networking.Abstractions.Threading;

public sealed class AsyncLock : IDisposable
{
   private readonly SemaphoreSlim _semaphore = new(1, 1);

   /// <summary>
   /// Acquires the lock. Returns a struct releaser that should be disposed.
   /// Allocation-free if the lock is immediately acquired.
   /// </summary>
   public async ValueTask<LockReleaser> LockAsync(CancellationToken cancellationToken = default)
   {
      await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
      return new LockReleaser(this);
   }

   internal void Release()
   {
      _semaphore.Release();
   }

   public void Dispose()
   {
      _semaphore.Dispose();
   }
}

public readonly struct LockReleaser(AsyncLock owner) : IDisposable
{
   private readonly AsyncLock _lock = owner;

   public void Dispose()
   {
      _lock?.Release();
   }
}

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Beskar.Networking.Abstractions.Extensions;

namespace Beskar.Networking.Transports.Common.Tests;

public class DisposableExtensionsTests
{
   private class MockAsyncDisposable(Func<ValueTask>? onDispose = null) : IAsyncDisposable
   {
      public int DisposeCount { get; private set; }

      public async ValueTask DisposeAsync()
      {
         DisposeCount++;
         if (onDispose is not null)
         {
            await onDispose();
         }
      }
   }

   private class MockDisposable(Action? onDispose = null) : IDisposable
   {
      public int DisposeCount { get; private set; }

      public void Dispose()
      {
         DisposeCount++;
         onDispose?.Invoke();
      }
   }

   [Test]
   public async Task DisposeAllAsync_DisposesAllAsyncDisposables()
   {
      var mock1 = new MockAsyncDisposable();
      var mock2 = new MockAsyncDisposable();
      var list = new List<IAsyncDisposable> { mock1, mock2 };

      await list.DisposeAllAsync();

      await Assert.That(mock1.DisposeCount).IsEqualTo(1);
      await Assert.That(mock2.DisposeCount).IsEqualTo(1);
   }

   [Test]
   public async Task DisposeAllAsync_DisposesAllSyncDisposables()
   {
      var mock1 = new MockDisposable();
      var mock2 = new MockDisposable();
      var list = new List<IDisposable> { mock1, mock2 };

      await list.DisposeAllAsync();

      await Assert.That(mock1.DisposeCount).IsEqualTo(1);
      await Assert.That(mock2.DisposeCount).IsEqualTo(1);
   }

   [Test]
   public async Task DisposeAllAsync_DisposesMixedDisposables()
   {
      var mockAsync = new MockAsyncDisposable();
      var mockSync = new MockDisposable();
      var list = new List<object> { mockAsync, mockSync };

      await list.DisposeAllAsync();

      await Assert.That(mockAsync.DisposeCount).IsEqualTo(1);
      await Assert.That(mockSync.DisposeCount).IsEqualTo(1);
   }

   [Test]
   public async Task DisposeAllAsync_GracefullyHandlesNull()
   {
      List<IAsyncDisposable>? list = null;
      await list.DisposeAllAsync();
   }

   [Test]
   public async Task DisposeAllAsync_ContinuesDisposingEvenIfOneThrows()
   {
      var mock1 = new MockAsyncDisposable(() => throw new InvalidOperationException("Oops"));
      var mock2 = new MockAsyncDisposable();
      var list = new List<IAsyncDisposable> { mock1, mock2 };

      await list.DisposeAllAsync();

      await Assert.That(mock1.DisposeCount).IsEqualTo(1);
      await Assert.That(mock2.DisposeCount).IsEqualTo(1);
   }
}

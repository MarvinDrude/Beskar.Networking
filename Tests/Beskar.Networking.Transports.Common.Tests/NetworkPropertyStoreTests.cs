using Beskar.Networking.Abstractions.Models;

namespace Beskar.Networking.Transports.Common.Tests;

public class NetworkPropertyStoreTests
{
   [Test]
   public async Task DefaultState_ReturnsEmptyReadOnlyDictionary_WithoutAllocatingInternalDictionary()
   {
      // Arrange & Act
      var store = new NetworkPropertyStore();

      // Assert
      await Assert.That(store.AllProperties).IsNotNull();
      await Assert.That(store.AllProperties.Count).IsEqualTo(0);
   }

   [Test]
   public async Task SetAndTryGet_WithDifferentTypes_StoresAndRetrievesCorrectly()
   {
      // Arrange
      var store = new NetworkPropertyStore();

      // Act
      store.Set("int-key", 42);
      store.Set("string-key", "hello");
      store.Set("bool-key", true);

      // Assert
      var getInt = store.TryGet("int-key", out int intVal);
      var getString = store.TryGet("string-key", out string? strVal);
      var getBool = store.TryGet("bool-key", out bool boolVal);

      await Assert.That(getInt).IsTrue();
      await Assert.That(intVal).IsEqualTo(42);

      await Assert.That(getString).IsTrue();
      await Assert.That(strVal).IsEqualTo("hello");

      await Assert.That(getBool).IsTrue();
      await Assert.That(boolVal).IsTrue();

      await Assert.That(store.AllProperties.Count).IsEqualTo(3);
   }

   [Test]
   public async Task TryGet_TypeMismatch_ReturnsFalseAndDefault()
   {
      // Arrange
      var store = new NetworkPropertyStore();
      store.Set("test-key", "not-an-int");

      // Act
      var result = store.TryGet("test-key", out int value);

      // Assert
      await Assert.That(result).IsFalse();
      await Assert.That(value).IsEqualTo(default);
   }

   [Test]
   public async Task Remove_ExistingKey_RemovesKeyAndReturnsTrue()
   {
      // Arrange
      var store = new NetworkPropertyStore();
      store.Set("test-key", "value");

      // Act
      var removed = store.Remove("test-key");
      var exists = store.TryGet("test-key", out string? value);

      // Assert
      await Assert.That(removed).IsTrue();
      await Assert.That(exists).IsFalse();
      await Assert.That(value).IsNull();
      await Assert.That(store.AllProperties.Count).IsEqualTo(0);
   }

   [Test]
   public async Task Remove_NonExistingKey_ReturnsFalse()
   {
      // Arrange
      var store = new NetworkPropertyStore();

      // Act
      var removed = store.Remove("non-existent");

      // Assert
      await Assert.That(removed).IsFalse();
   }

   [Test]
   public async Task Clear_ClearsAllKeys()
   {
      // Arrange
      var store = new NetworkPropertyStore();
      store.Set("key1", 1);
      store.Set("key2", 2);

      // Act
      store.Clear();

      // Assert
      await Assert.That(store.AllProperties.Count).IsEqualTo(0);
      await Assert.That(store.TryGet("key1", out int _)).IsFalse();
   }

   [Test]
   public async Task ThreadSafety_MultipleThreadsAccessConcurrently_DoesNotCrash()
   {
      // Arrange
      var store = new NetworkPropertyStore();
      const int taskCount = 10;
      const int operationsPerTask = 100;
      var tasks = new Task[taskCount];

      // Act
      for (var i = 0; i < taskCount; i++)
      {
         var taskId = i;
         tasks[i] = Task.Run(() =>
         {
            for (var j = 0; j < operationsPerTask; j++)
            {
               var key = $"key-{taskId}-{j}";
               store.Set(key, j);
               if (store.TryGet<int>(key, out var val))
                  if (val != j)
                     throw new Exception("Data corruption detected!");
            }
         });
      }

      await Task.WhenAll(tasks);

      // Assert
      await Assert.That(store.AllProperties.Count).IsEqualTo(taskCount * operationsPerTask);
   }
}

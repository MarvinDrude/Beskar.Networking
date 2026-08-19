using System.Text;
using Beskar.Networking.Raft.StateMachine;

namespace Beskar.Networking.Raft.Tests;

public class RaftStateMachineTests
{
   [Test]
   public async Task DefaultStateMachine_DefaultSnapshotMethods_ReturnEmptyAndCompleted()
   {
      IRaftStateMachine sm = new DefaultStateMachine();

      var snapshot = await sm.TakeSnapshotAsync();
      await Assert.That(snapshot.IsEmpty).IsTrue();

      await sm.RestoreSnapshotAsync(ReadOnlyMemory<byte>.Empty, 10, 2);
   }

   [Test]
   [Arguments("SET key1=val1", "OK")]
   [Arguments("SET key2=val2", "OK")]
   [Arguments("SET key3=", "OK")]
   [Arguments("INVALID_COMMAND", "OK")]
   [Arguments("", "OK")]
   public async Task KeyValueStateMachine_ApplyCommands_ReturnsResult(string commandText, string expectedResult)
   {
      var sm = new SnapshotStateMachine();
      var result = await sm.ApplyAsync(Encoding.UTF8.GetBytes(commandText), 1);

      await Assert.That(Encoding.UTF8.GetString(result.Span)).IsEqualTo(expectedResult);
   }

   [Test]
   public async Task KeyValueStateMachine_MultipleSnapshotCycles_MaintainIntegrity()
   {
      var sm = new SnapshotStateMachine();

      for (var cycle = 1; cycle <= 5; cycle++)
      {
         for (var i = 1; i <= 20; i++)
            await sm.ApplyAsync(Encoding.UTF8.GetBytes($"SET cycle{cycle}_k{i}=v{i}"), (ulong)(cycle * 20 + i));

         var snapshot = await sm.TakeSnapshotAsync();

         var restoredSm = new SnapshotStateMachine();
         await restoredSm.RestoreSnapshotAsync(snapshot, (ulong)(cycle * 20 + 20), (ulong)cycle);

         await Assert.That(restoredSm.Store.Count).IsEqualTo(cycle * 20);
      }
   }

   [Test]
   public async Task KeyValueStateMachine_RestoreEmptySnapshot_DoesNotThrow()
   {
      var sm = new SnapshotStateMachine();
      await sm.ApplyAsync("SET k1=v1"u8.ToArray(), 1);

      await sm.RestoreSnapshotAsync(ReadOnlyMemory<byte>.Empty, 0, 0);

      await Assert.That(sm.Store.Count).IsEqualTo(1);
   }

   private sealed class DefaultStateMachine : IRaftStateMachine
   {
      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
         CancellationToken ct = default)
      {
         return ValueTask.FromResult(command);
      }
   }
}

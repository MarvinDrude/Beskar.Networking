using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Common.Tests.Helpers;

public class DummyNetworkStream : INetworkStream
{
   private readonly AsyncLock _lock = new();

   public long StreamId => 0;
   public INetworkSession Session { get; } = new DummyNetworkSession();
   public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
   public IDuplexPipe Transport => throw new NotImplementedException();

   public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
   {
      return _lock.LockAsync(cancellationToken);
   }

   public ValueTask DisposeAsync()
   {
      return ValueTask.CompletedTask;
   }
}

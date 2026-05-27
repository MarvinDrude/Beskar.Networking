using System.IO.Pipelines;
using Beskar.Networking.Transports.Common.Settings;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Options;

public sealed class SocketTransportOptions
   : BaseTransportOptions<SocketQueueSettings>
{
   public override SocketQueueSettings CreateQueueSettings()
   {
      var memoryPool = new PinnedBlockMemoryPool();
      var scheduler = new IoQueue();

      var maxReadBufferSize = MaxReadBufferSize ?? 0;
      var maxWriteBufferSize = MaxWriteBufferSize ?? 0;

      return new SocketQueueSettings()
      {
         MemoryPool = memoryPool,
         PipeScheduler = scheduler,

         ReceiveOptions = new PipeOptions(
            memoryPool, PipeScheduler.ThreadPool, PipeScheduler.ThreadPool,
            maxReadBufferSize, maxReadBufferSize / 2,
            useSynchronizationContext: false),
         SendOptions = new PipeOptions(
            memoryPool, PipeScheduler.ThreadPool, PipeScheduler.ThreadPool,
            maxWriteBufferSize, maxWriteBufferSize / 2,
            useSynchronizationContext: false),
      };
   }
}

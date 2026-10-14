using System.IO.Pipelines;
using Beskar.Networking.Transports.Common.Settings;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Options;

public sealed class SocketTransportOptions
   : BaseTransportOptions<SocketQueueSettings>
{
   public override SocketQueueSettings CreateQueueSettings()
   {
      var memoryPool = SharedTransportMemoryPool.GetNext();
      var scheduler = new IoQueue();

      var pauseRead = MaxReadBufferSize ?? -1;
      var resumeRead = MaxReadBufferSize.HasValue ? MaxReadBufferSize.Value / 2 : -1;

      var pauseWrite = MaxWriteBufferSize ?? -1;
      var resumeWrite = MaxWriteBufferSize.HasValue ? MaxWriteBufferSize.Value / 2 : -1;

      return new SocketQueueSettings()
      {
         MemoryPool = memoryPool,
         OwnsMemoryPool = false,
         PipeScheduler = scheduler,

         ReceiveOptions = new PipeOptions(
            memoryPool, scheduler, scheduler,
            pauseRead, resumeRead,
            useSynchronizationContext: false),
         SendOptions = new PipeOptions(
            memoryPool, scheduler, scheduler,
            pauseWrite, resumeWrite,
            useSynchronizationContext: false),
      };
   }
}

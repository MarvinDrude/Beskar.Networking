using System.IO.Pipelines;
using Beskar.Networking.Transports.Common.Settings;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Options;

public sealed class StreamTransportOptions
   : BaseTransportOptions<StreamQueueSettings>
{
   public override StreamQueueSettings CreateQueueSettings()
   {
      var memoryPool = SharedTransportMemoryPool.GetNext();

      var pauseRead = MaxReadBufferSize ?? -1;
      var resumeRead = MaxReadBufferSize.HasValue ? MaxReadBufferSize.Value / 2 : -1;
      var pauseWrite = MaxWriteBufferSize ?? -1;
      var resumeWrite = MaxWriteBufferSize.HasValue ? MaxWriteBufferSize.Value / 2 : -1;

      return new StreamQueueSettings()
      {
         MemoryPool = memoryPool,
         OwnsMemoryPool = false,

         ReceiveOptions = new PipeOptions(
            memoryPool, PipeScheduler.ThreadPool, PipeScheduler.ThreadPool,
            pauseRead, resumeRead,
            useSynchronizationContext: false),
         SendOptions = new PipeOptions(
            memoryPool, PipeScheduler.ThreadPool, PipeScheduler.ThreadPool,
            pauseWrite, resumeWrite,
            useSynchronizationContext: false),
      };
   }
}

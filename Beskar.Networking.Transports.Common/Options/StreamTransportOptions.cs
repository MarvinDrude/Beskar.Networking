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

      var maxReadBufferSize = MaxReadBufferSize ?? 0;
      var maxWriteBufferSize = MaxWriteBufferSize ?? 0;

      return new StreamQueueSettings()
      {
         MemoryPool = memoryPool,
         OwnsMemoryPool = false,

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

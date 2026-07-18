using System.Buffers;
using System.IO.Pipelines;

namespace Beskar.Networking.Transports.Common.Settings;

/// <summary>
/// Represents the base settings for a queue.
/// </summary>
public class BaseQueueSettings : IDisposable
{
   /// <summary>
   /// The options for receiving data from the queue.
   /// </summary>
   public required PipeOptions ReceiveOptions { get; set; }

   /// <summary>
   /// The options for sending data to the queue.
   /// </summary>
   public required PipeOptions SendOptions { get; set; }

   /// <summary>
   /// The memory pool used for the queue.
   /// </summary>
   public required MemoryPool<byte> MemoryPool { get; set; }

   /// <summary>
   /// Whether this settings object owns the lifetime of the MemoryPool and should dispose it.
   /// </summary>
   public bool OwnsMemoryPool { get; set; } = true;

   private bool _isDisposed;

   public virtual void Dispose()
   {
      if (_isDisposed) return;
      _isDisposed = true;

      if (OwnsMemoryPool)
      {
         MemoryPool.Dispose();
      }
   }
}

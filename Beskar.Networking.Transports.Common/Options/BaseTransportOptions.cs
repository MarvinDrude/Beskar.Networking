using System.Net.Security;
using Beskar.Networking.Transports.Common.Settings;

namespace Beskar.Networking.Transports.Common.Options;

public abstract class BaseTransportOptions<TQueueSelf>
   where TQueueSelf : BaseQueueSettings
{
   /// <summary>
   /// The SSL options for the transport.
   /// </summary>
   public SslServerAuthenticationOptions? SslServerOptions { get; set; }

   /// <summary>
   /// The SSL client options for the transport.
   /// </summary>
   public SslClientAuthenticationOptions? SslClientOptions { get; set; }

   /// <summary>
   /// The number of IO queues for the transport.
   /// </summary>
   public int IoQueueCount { get; set; } = Math.Min(Environment.ProcessorCount, 12);

   /// <summary>
   /// The maximum size of the read buffer.
   /// </summary>
   public long? MaxReadBufferSize { get; set; } = 1024 * 1024;

   /// <summary>
   /// The maximum size of the write buffer.
   /// </summary>
   public long? MaxWriteBufferSize { get; set; } = 64 * 1024;

   /// <summary>
   /// The maximum number of connections in the connection pool.
   /// </summary>
   public int MaxConnectionPoolSize { get; set; } = 2048;

   /// <summary>
   /// The initial number of connections in the connection pool.
   /// </summary>
   public int InitialConnectionPoolSize { get; set; } = 16;

   public abstract TQueueSelf CreateQueueSettings();
}

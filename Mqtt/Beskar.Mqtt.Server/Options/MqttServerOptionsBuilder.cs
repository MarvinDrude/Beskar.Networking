using System;
using Beskar.Mqtt.Server.Enums;

namespace Beskar.Mqtt.Server.Options;

/// <summary>
/// A builder for constructing instances of <see cref="MqttServerOptions"/>.
/// </summary>
public sealed class MqttServerOptionsBuilder
{
   private TimeSpan _keepAliveInterval = TimeSpan.FromMilliseconds(1_000);
   private bool _supportPersistentSessions;
   private MessageOverflowBehavior _pendingMessageOverflowBehavior = MessageOverflowBehavior.DropOldest;
   private ushort _maxPendingMessagesPerConnection = 1024;

   private int _maxConnections;
   private uint _maximumPacketSize;

   /// <summary>
   /// Sets the interval at which the server will check the keep alive states of all connected clients.
   /// </summary>
   /// <param name="interval">The keep-alive check interval.</param>
   /// <returns>The builder instance for chaining.</returns>
   public MqttServerOptionsBuilder WithKeepAliveInterval(TimeSpan interval)
   {
      ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
      _keepAliveInterval = interval;
      return this;
   }

   /// <summary>
   /// Sets whether persistent sessions are enabled on the server.
   /// </summary>
   /// <param name="supportPersistentSessions"><c>true</c> to support persistent sessions; otherwise, <c>false</c>.</param>
   /// <returns>The builder instance for chaining.</returns>
   public MqttServerOptionsBuilder WithSupportPersistentSessions(bool supportPersistentSessions = true)
   {
      _supportPersistentSessions = supportPersistentSessions;
      return this;
   }

   /// <summary>
   /// Sets the behavior to use when the pending message queue overflows.
   /// </summary>
   /// <param name="behavior">The overflow behavior to apply.</param>
   /// <returns>The builder instance for chaining.</returns>
   public MqttServerOptionsBuilder WithPendingMessageOverflowBehavior(MessageOverflowBehavior behavior)
   {
      _pendingMessageOverflowBehavior = behavior;
      return this;
   }

   /// <summary>
   /// Sets the maximum number of pending messages per client connection.
   /// </summary>
   /// <param name="maxPendingMessagesPerConnection">The maximum number of pending messages.</param>
   /// <returns>The builder instance for chaining.</returns>
   public MqttServerOptionsBuilder WithMaxPendingMessagesPerConnection(ushort maxPendingMessagesPerConnection)
   {
      _maxPendingMessagesPerConnection = maxPendingMessagesPerConnection;
      return this;
   }

   /// <summary>
   /// Sets the maximum number of concurrent client connections allowed by the server.
   /// </summary>
   /// <param name="maxConnections">The maximum connection count (<c>0</c> for unlimited).</param>
   /// <returns>The builder instance for chaining.</returns>
   public MqttServerOptionsBuilder WithMaxConnections(int maxConnections)
   {
      ArgumentOutOfRangeException.ThrowIfNegative(maxConnections);
      _maxConnections = maxConnections;
      return this;
   }

   /// <summary>
   /// Sets the maximum MQTT packet size in bytes accepted by the server.
   /// </summary>
   /// <param name="maximumPacketSize">The maximum packet size (<c>0</c> for no protocol limit).</param>
   /// <returns>The builder instance for chaining.</returns>
   public MqttServerOptionsBuilder WithMaximumPacketSize(uint maximumPacketSize)
   {
      _maximumPacketSize = maximumPacketSize;
      return this;
   }

   /// <summary>
   /// Builds and returns the configured <see cref="MqttServerOptions"/> instance.
   /// </summary>
   /// <returns>A new <see cref="MqttServerOptions"/> instance.</returns>
   public MqttServerOptions Build()
   {
      return new MqttServerOptions
      {
         KeepAlive = new MqttServerKeepAliveOptions
         {
            Interval = _keepAliveInterval
         },
         SupportPersistentSessions = _supportPersistentSessions,
         PendingMessageOverflowBehavior = _pendingMessageOverflowBehavior,
         MaxPendingMessagesPerConnection = _maxPendingMessagesPerConnection,
         MaxConnections = _maxConnections,
         MaximumPacketSize = _maximumPacketSize
      };
   }
}

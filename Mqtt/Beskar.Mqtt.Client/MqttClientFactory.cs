using Beskar.Mqtt.Common.Interfaces;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Client;

/// <summary>
/// A factory for creating instances of <see cref="IMqttClient"/> configured with various transport types.
/// </summary>
public abstract class MqttClientFactory : IClientFactory<IMqttClient>
{
   protected MqttClientFactory()
   {
   }

   /// <summary>
   /// Creates a MQTT client instance given a network client.
   /// If you don't know what a network client is, you can use one of the factory methods to create one or
   /// use CreateTcp, CreateWs, or CreateQuic to create a mqtt client.
   /// </summary>
   /// <param name="networkClient"></param>
   /// <returns></returns>
   public static IMqttClient Create(INetworkClient networkClient)
   {
      TraceLogger.LogClientInfo("MqttClientFactory: Creating Custom MQTT client.");
      return new MqttClient(networkClient);
   }

   /// <summary>
   /// Creates an MQTT client instance that communicates over a standard TCP connection.
   /// </summary>
   /// <param name="options">Optional TCP transport options configuration.</param>
   /// <returns>A new <see cref="IMqttClient"/> instance configured with a TCP client transport.</returns>
   public static IMqttClient CreateTcp(TcpTransportOptions? options = null)
   {
      TraceLogger.LogClientInfo("MqttClientFactory: Creating TCP MQTT client.");

      options ??= new TcpTransportOptions();
      options.StreamOptions.IoQueueCount = 1;
      options.SocketOptions.IoQueueCount = 1;

      var underlying = new TcpNetworkClient(options);
      return new MqttClient(underlying);
   }

   /// <summary>
   /// Creates an MQTT client instance that communicates over a WebSocket connection.
   /// </summary>
   /// <param name="options">Optional WebSocket transport options configuration.</param>
   /// <returns>A new <see cref="IMqttClient"/> instance configured with a WebSocket client transport.</returns>
   public static IMqttClient CreateWs(WsTransportOptions? options = null)
   {
      TraceLogger.LogClientInfo("MqttClientFactory: Creating WebSocket MQTT client.");

      options ??= new WsTransportOptions();
      options.TcpOptions.StreamOptions.IoQueueCount = 1;
      options.TcpOptions.SocketOptions.IoQueueCount = 1;

      var underlying = new WsNetworkClient(options);
      return new MqttClient(underlying);
   }

   /// <summary>
   /// Creates an MQTT client instance that communicates over a multiplexed QUIC connection.
   /// </summary>
   /// <param name="options">Optional QUIC transport options configuration.</param>
   /// <returns>A new <see cref="IMqttClient"/> instance configured with a QUIC client transport.</returns>
   public static IMqttClient CreateQuic(QuicTransportOptions? options = null)
   {
      TraceLogger.LogClientInfo("MqttClientFactory: Creating QUIC MQTT client.");

      options ??= new QuicTransportOptions();
      options.StreamOptions.IoQueueCount = 1;

      var underlying = new QuicNetworkClient(options);
      return new MqttClient(underlying);
   }
}

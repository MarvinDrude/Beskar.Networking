using Beskar.Mqtt.Common.Interfaces;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Mqtt.Client;

/// <summary>
/// A factory for creating instances of <see cref="IMqttClient"/> configured with various transport types.
/// </summary>
public static class MqttClientFactory
{
   /// <summary>
   /// Creates an MQTT client instance that communicates over a standard TCP connection.
   /// </summary>
   /// <param name="options">Optional TCP transport options configuration.</param>
   /// <returns>A new <see cref="IMqttClient"/> instance configured with a TCP client transport.</returns>
   public static IMqttClient CreateTcp(TcpTransportOptions? options = null)
   {
      var underlying = new TcpNetworkClient(options ?? new TcpTransportOptions());
      return new MqttClient(underlying);
   }

   /// <summary>
   /// Creates an MQTT client instance that communicates over a WebSocket connection.
   /// </summary>
   /// <param name="options">Optional WebSocket transport options configuration.</param>
   /// <returns>A new <see cref="IMqttClient"/> instance configured with a WebSocket client transport.</returns>
   public static IMqttClient CreateWs(WsTransportOptions? options = null)
   {
      var underlying = new WsNetworkClient(options ?? new WsTransportOptions());
      return new MqttClient(underlying);
   }

   /// <summary>
   /// Creates an MQTT client instance that communicates over a multiplexed QUIC connection.
   /// </summary>
   /// <param name="options">Optional QUIC transport options configuration.</param>
   /// <returns>A new <see cref="IMqttClient"/> instance configured with a QUIC client transport.</returns>
   public static IMqttClient CreateQuic(QuicTransportOptions? options = null)
   {
      var underlying = new QuicNetworkClient(options ?? new QuicTransportOptions());
      return new MqttClient(underlying);
   }
}

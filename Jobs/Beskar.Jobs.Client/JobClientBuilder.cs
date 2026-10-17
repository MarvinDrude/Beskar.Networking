using System.Net;
using Beskar.Jobs.Client.Options;
using Beskar.Jobs.Common.Serialization;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Jobs.Client;

/// <summary>
/// Fluent builder for creating and configuring <see cref="JobClient"/> instances.
/// </summary>
public sealed class JobClientBuilder
{
   private readonly JobClientOptions _options = new();
   private IMqttClient? _mqttClient;
   private bool _ownsClient = true;

   private JobClientBuilder()
   {
   }

   public static JobClientBuilder Create() => new();

   public JobClientBuilder WithClientId(string clientId)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
      _options.ClientId = clientId;
      return this;
   }

   public JobClientBuilder WithRequestTimeout(TimeSpan timeout)
   {
      _options.RequestTimeout = timeout;
      return this;
   }

   public JobClientBuilder WithSerializer(IJobPayloadSerializer serializer)
   {
      ArgumentNullException.ThrowIfNull(serializer);
      _options.Serializer = serializer;
      return this;
   }

   public JobClientBuilder UseTcp(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateTcp();
      _ownsClient = true;
      return this;
   }

   public JobClientBuilder UseTcp(int port) => UseTcp(new IPEndPoint(IPAddress.Loopback, port));

   public JobClientBuilder UseWs(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateWs();
      _ownsClient = true;
      return this;
   }

   public JobClientBuilder UseQuic(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateQuic();
      _ownsClient = true;
      return this;
   }

   public JobClientBuilder UseTransport(INetworkClient networkClient, IPEndPoint endPoint)
   {
      ArgumentNullException.ThrowIfNull(networkClient);
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.Create(networkClient);
      _ownsClient = true;
      return this;
   }

   public JobClientBuilder UseMqttClient(IMqttClient mqttClient, IPEndPoint? endPoint = null, bool disposeClient = false)
   {
      ArgumentNullException.ThrowIfNull(mqttClient);
      _mqttClient = mqttClient;
      _options.EndPoint = endPoint;
      _ownsClient = disposeClient;
      return this;
   }

   public JobClient Build()
   {
      var client = _mqttClient ?? MqttClientFactory.CreateTcp();
      return new JobClient(client, _options, _ownsClient);
   }
}

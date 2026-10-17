using System.Net;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Common.Serialization;
using Beskar.Jobs.Common.Storage;
using Beskar.Jobs.Scheduler.Options;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Jobs.Scheduler;

/// <summary>
/// Fluent builder for configuring and creating <see cref="JobScheduler"/> instances.
/// </summary>
public sealed class JobSchedulerBuilder
{
   private readonly JobSchedulerOptions _options = new();
   private IJobStorage? _storage;
   private IMqttClient? _mqttClient;
   private bool _ownsClient = true;

   private JobSchedulerBuilder()
   {
   }

   public static JobSchedulerBuilder Create() => new();

   public JobSchedulerBuilder WithSchedulerId(string schedulerId)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(schedulerId);
      _options.SchedulerId = schedulerId;
      return this;
   }

   public JobSchedulerBuilder WithTickInterval(TimeSpan tickInterval)
   {
      ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(tickInterval, TimeSpan.Zero);
      _options.TickInterval = tickInterval;
      return this;
   }

   public JobSchedulerBuilder WithDispatchBatchSize(int batchSize)
   {
      ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
      _options.DispatchBatchSize = batchSize;
      return this;
   }

   public JobSchedulerBuilder WithDispatchAckTimeout(TimeSpan timeout)
   {
      _options.DispatchAckTimeout = timeout;
      return this;
   }

   public JobSchedulerBuilder WithRetryPolicy(RetryPolicy retryPolicy)
   {
      ArgumentNullException.ThrowIfNull(retryPolicy);
      _options.RetryPolicy = retryPolicy;
      return this;
   }

   public JobSchedulerBuilder WithSerializer(IJobPayloadSerializer serializer)
   {
      ArgumentNullException.ThrowIfNull(serializer);
      _options.Serializer = serializer;
      return this;
   }

   public JobSchedulerBuilder UseStorage(IJobStorage storage)
   {
      ArgumentNullException.ThrowIfNull(storage);
      _storage = storage;
      return this;
   }

   public JobSchedulerBuilder UseTcp(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateTcp();
      _ownsClient = true;
      return this;
   }

   public JobSchedulerBuilder UseTcp(int port) => UseTcp(new IPEndPoint(IPAddress.Loopback, port));

   public JobSchedulerBuilder UseWs(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateWs();
      _ownsClient = true;
      return this;
   }

   public JobSchedulerBuilder UseQuic(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateQuic();
      _ownsClient = true;
      return this;
   }

   public JobSchedulerBuilder UseTransport(INetworkClient networkClient, IPEndPoint endPoint)
   {
      ArgumentNullException.ThrowIfNull(networkClient);
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.Create(networkClient);
      _ownsClient = true;
      return this;
   }

   public JobSchedulerBuilder UseMqttClient(IMqttClient mqttClient, IPEndPoint? endPoint = null, bool disposeClient = false)
   {
      ArgumentNullException.ThrowIfNull(mqttClient);
      _mqttClient = mqttClient;
      _options.EndPoint = endPoint;
      _ownsClient = disposeClient;
      return this;
   }

   public JobScheduler Build()
   {
      var client = _mqttClient ?? MqttClientFactory.CreateTcp();
      var storage = _storage ?? new InMemoryJobStorage();
      return new JobScheduler(client, storage, _options, _ownsClient);
   }
}

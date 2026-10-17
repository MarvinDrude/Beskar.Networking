using System.Net;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Common.Serialization;
using Beskar.Jobs.Worker.Internal;
using Beskar.Jobs.Worker.Options;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Jobs.Worker;

/// <summary>
/// Fluent builder for configuring and creating <see cref="JobWorker"/> instances.
/// </summary>
public sealed class JobWorkerBuilder
{
   private readonly JobWorkerOptions _options = new();
   private readonly Dictionary<string, JobHandlerExecutor> _handlers = new(StringComparer.Ordinal);

   private IMqttClient? _mqttClient;
   private bool _ownsClient = true;

   private JobWorkerBuilder()
   {
   }

   public static JobWorkerBuilder Create() => new();

   public JobWorkerBuilder WithWorkerId(string workerId)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(workerId);
      _options.WorkerId = workerId;
      return this;
   }

   public JobWorkerBuilder WithMaxConcurrency(int maxConcurrency)
   {
      ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
      _options.MaxConcurrency = maxConcurrency;
      return this;
   }

   public JobWorkerBuilder WithHeartbeatInterval(TimeSpan interval)
   {
      _options.HeartbeatInterval = interval;
      return this;
   }

   public JobWorkerBuilder WithKeepAlivePeriod(ushort seconds)
   {
      _options.KeepAlivePeriodSeconds = seconds;
      return this;
   }

   public JobWorkerBuilder WithSerializer(IJobPayloadSerializer serializer)
   {
      ArgumentNullException.ThrowIfNull(serializer);
      _options.Serializer = serializer;
      return this;
   }

   public JobWorkerBuilder SubscribeQueue(params ReadOnlySpan<string> queues)
   {
      foreach (var queue in queues)
      {
         if (!string.IsNullOrWhiteSpace(queue) && !_options.Queues.Contains(queue, StringComparer.Ordinal))
         {
            _options.Queues.Add(queue);
         }
      }

      return this;
   }

   /// <summary>
   /// Registers a strongly-typed <see cref="IJobHandler{TArgs}"/> instance for the given <paramref name="jobType"/>.
   /// </summary>
   public JobWorkerBuilder RegisterHandler<TArgs>(string jobType, IJobHandler<TArgs> handler)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(jobType);
      ArgumentNullException.ThrowIfNull(handler);

      return RegisterHandler<TArgs>(jobType, (ctx, ct) => handler.ExecuteAsync(ctx, ct));
   }

   /// <summary>
   /// Registers a strongly-typed <see cref="IJobHandler{TArgs}"/> class with a parameterless constructor.
   /// </summary>
   public JobWorkerBuilder RegisterHandler<TArgs, THandler>(string jobType)
      where THandler : IJobHandler<TArgs>, new()
   {
      return RegisterHandler(jobType, new THandler());
   }

   /// <summary>
   /// Registers a source-generated job handler annotated with <c>[GeneratedJobHandler]</c> and subscribes to its default queue.
   /// </summary>
   public JobWorkerBuilder RegisterGeneratedHandler<THandler, TArgs>(THandler? instance = null)
      where THandler : class, IGeneratedJobDescriptor, IJobHandler<TArgs>, new()
   {
      SubscribeQueue(THandler.GeneratedQueue);
      return RegisterHandler(THandler.GeneratedJobType, instance ?? new THandler());
   }

   /// <summary>
   /// Registers a delegate handler for the specified <paramref name="jobType"/>.
   /// </summary>
   public JobWorkerBuilder RegisterHandler<TArgs>(
      string jobType,
      Func<IJobExecutionContext<TArgs>, CancellationToken, ValueTask> handler)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(jobType);
      ArgumentNullException.ThrowIfNull(handler);

      _handlers[jobType] = async (dispatchReq, progressReporter, ct) =>
      {
         var args = _options.Serializer.Deserialize<TArgs>(dispatchReq.Payload)!;
         var ctx = new JobExecutionContext<TArgs>(
            dispatchReq.JobId,
            dispatchReq.JobType,
            dispatchReq.Queue,
            dispatchReq.Attempt,
            dispatchReq.MaxAttempts,
            _options.WorkerId,
            args,
            dispatchReq.Metadata,
            progressReporter);

         await handler(ctx, ct);
         return null;
      };

      return this;
   }

   /// <summary>
   /// Registers a strongly-typed <see cref="IJobHandlerWithResult{TArgs, TResult}"/> instance that produces a result payload.
   /// </summary>
   public JobWorkerBuilder RegisterHandlerWithResult<TArgs, TResult>(
      string jobType,
      IJobHandlerWithResult<TArgs, TResult> handler)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(jobType);
      ArgumentNullException.ThrowIfNull(handler);

      return RegisterHandlerWithResult<TArgs, TResult>(jobType, handler.ExecuteAsync);
   }

   /// <summary>
   /// Registers a strongly-typed <see cref="IJobHandlerWithResult{TArgs, TResult}"/> class with a parameterless constructor.
   /// </summary>
   public JobWorkerBuilder RegisterHandlerWithResult<TArgs, TResult, THandler>(string jobType)
      where THandler : IJobHandlerWithResult<TArgs, TResult>, new()
   {
      return RegisterHandlerWithResult<TArgs, TResult>(jobType, new THandler());
   }

   /// <summary>
   /// Registers a delegate handler that returns a typed <typeparamref name="TResult"/> upon completion.
   /// </summary>
   public JobWorkerBuilder RegisterHandlerWithResult<TArgs, TResult>(
      string jobType,
      Func<IJobExecutionContext<TArgs>, CancellationToken, ValueTask<TResult>> handler)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(jobType);
      ArgumentNullException.ThrowIfNull(handler);

      _handlers[jobType] = async (dispatchReq, progressReporter, ct) =>
      {
         var args = _options.Serializer.Deserialize<TArgs>(dispatchReq.Payload)!;
         var ctx = new JobExecutionContext<TArgs>(
            dispatchReq.JobId,
            dispatchReq.JobType,
            dispatchReq.Queue,
            dispatchReq.Attempt,
            dispatchReq.MaxAttempts,
            _options.WorkerId,
            args,
            dispatchReq.Metadata,
            progressReporter);

         var result = await handler(ctx, ct);
         return _options.Serializer.Serialize(result);
      };

      return this;
   }

   public JobWorkerBuilder UseTcp(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateTcp();
      _ownsClient = true;
      return this;
   }

   public JobWorkerBuilder UseTcp(int port) => UseTcp(new IPEndPoint(IPAddress.Loopback, port));

   public JobWorkerBuilder UseWs(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateWs();
      _ownsClient = true;
      return this;
   }

   public JobWorkerBuilder UseQuic(IPEndPoint endPoint)
   {
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.CreateQuic();
      _ownsClient = true;
      return this;
   }

   public JobWorkerBuilder UseTransport(INetworkClient networkClient, IPEndPoint endPoint)
   {
      ArgumentNullException.ThrowIfNull(networkClient);
      _options.EndPoint = endPoint;
      _mqttClient = MqttClientFactory.Create(networkClient);
      _ownsClient = true;
      return this;
   }

   public JobWorkerBuilder UseMqttClient(IMqttClient mqttClient, IPEndPoint? endPoint = null, bool disposeClient = false)
   {
      ArgumentNullException.ThrowIfNull(mqttClient);
      _mqttClient = mqttClient;
      _options.EndPoint = endPoint;
      _ownsClient = disposeClient;
      return this;
   }

   public JobWorker Build()
   {
      var client = _mqttClient ?? MqttClientFactory.CreateTcp();
      return new JobWorker(client, _options, new Dictionary<string, JobHandlerExecutor>(_handlers, StringComparer.Ordinal), _ownsClient);
   }
}

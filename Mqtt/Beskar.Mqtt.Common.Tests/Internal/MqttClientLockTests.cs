using System.Buffers;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.IO.Pipelines;
using Beskar.Memory.Results;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttClientLockTests
{
   [Test]
   public async Task PublishAsync_WhenCancelledDuringAwaitAck_ShouldNotDoubleReleaseLock()
   {
      var mockNetworkClient = new MockNetworkClient();
      var client = new MqttClient(mockNetworkClient);

      // Force state to Connected (3)
      var stateField = typeof(MqttClient).GetField("_state", BindingFlags.NonPublic | BindingFlags.Instance);
      stateField?.SetValue(client, 3); // MqttClientConnectionState.Connected = 3

      // Set protocol version to V50
      var versionField = typeof(MqttClient).GetField("_protocolVersion", BindingFlags.NonPublic | BindingFlags.Instance);
      versionField?.SetValue(client, MqttProtocolVersion.V50);

      // Set connect options
      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, 1883)
      };
      var optionsField = typeof(MqttClient).GetField("_connectOptions", BindingFlags.NonPublic | BindingFlags.Instance);
      optionsField?.SetValue(client, connectOptions);

      // Set mock control stream
      using var cts = new CancellationTokenSource();
      var stream = new CustomMockNetworkStream(cts);
      var streamField = typeof(MqttClient).GetField("_controlStream", BindingFlags.NonPublic | BindingFlags.Instance);
      streamField?.SetValue(client, stream);

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/topic")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("payload")
         .Build();

      // Trigger publish - this will cancel the cts during FlushAsync, causing AwaitAck to throw an exception
      var publishResult = await client.PublishAsync(pubOptions, cts.Token).WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(publishResult.Failed).IsTrue();

      // Assert that we CANNOT acquire the lock twice concurrently (i.e. it was not double-released)
      using (var lock1 = await stream.Lock.LockAsync(CancellationToken.None))
      {
         var lock2Task = stream.Lock.LockAsync(CancellationToken.None).AsTask();
         var completedTask = await Task.WhenAny(lock2Task, Task.Delay(200));

         await Assert.That(completedTask != lock2Task).IsTrue();
      }
   }

   private class MockNetworkClient : INetworkClient
   {
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsConnected => false;
      public NetworkClientStats Stats => default;
      public INetworkSession? Session => null;
      public EndPoint? LocalAddress => null;
      public EndPoint? RemoteAddress => null;
      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default)
      {
         return ValueTask.FromResult<Result<INetworkSession, NetworkCodeError>>(new NetworkCodeError(1, "Mock connect not supported"));
      }
      public ValueTask DisconnectAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private class DummyNetworkSession : INetworkSession
   {
      public Guid Id { get; } = Guid.NewGuid();
      public EndPoint RemoteAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
      public EndPoint LocalAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
      public bool IsSupportingMultiplexing => false;
      public bool IsSupportingUnidirectional => false;
      public CancellationToken SessionClosedToken => CancellationToken.None;
      public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();
      public NetworkStats Stats => default;
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public TransportKind Transport => TransportKind.Unknown;
      public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);
      public NetworkSessionStats SessionStats => default;
      public IReadOnlyCollection<INetworkStream> ActiveStreams => Array.Empty<INetworkStream>();
      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default) => throw new NotImplementedException();
      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default) => throw new NotImplementedException();
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private class MockDuplexPipe : IDuplexPipe
   {
      public MockDuplexPipe(PipeReader reader, PipeWriter writer)
      {
         Input = reader;
         Output = writer;
      }
      public PipeReader Input { get; }
      public PipeWriter Output { get; }
   }

   private class CancelOnFlushWriter(PipeWriter inner, CancellationTokenSource cts) : PipeWriter
   {
      private readonly PipeWriter _inner = inner;
      private readonly CancellationTokenSource _cts = cts;

      public override void Advance(int bytes) => _inner.Advance(bytes);
      public override Memory<byte> GetMemory(int sizeHint = 0) => _inner.GetMemory(sizeHint);
      public override Span<byte> GetSpan(int sizeHint = 0) => _inner.GetSpan(sizeHint);
      public override void CancelPendingFlush() => _inner.CancelPendingFlush();
      public override void Complete(Exception? exception = null) => _inner.Complete(exception);

      public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
      {
         var res = await _inner.FlushAsync(cancellationToken);
         await _cts.CancelAsync(); // Cancel the token source upon flush
         return res;
      }
   }

   private class CustomMockNetworkStream : INetworkStream
   {
      public readonly AsyncLock Lock = new();
      private readonly Pipe _pipe = new();
      private readonly CancelOnFlushWriter _writer;

      public CustomMockNetworkStream(CancellationTokenSource cts)
      {
         _writer = new CancelOnFlushWriter(_pipe.Writer, cts);
      }

      public long StreamId => 1;
      public INetworkSession Session { get; } = new DummyNetworkSession();
      public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
      public IDuplexPipe Transport => new MockDuplexPipe(_pipe.Reader, _writer);
      public NetworkStats Stats { get; set; }
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default) => Lock.LockAsync(cancellationToken);
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }
}

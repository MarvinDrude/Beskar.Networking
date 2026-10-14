using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Managed;
using Beskar.Networking.Abstractions.Options;
using Beskar.Networking.Transports.Common.Options;
using Beskar.Networking.Transports.Common.Pipelines;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;
using Beskar.Networking.Transports.Ws.Enums;
using Beskar.Utilities.Console.Rendering;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.Common.Tests;

[NotInParallel]
public class TransportCriticalBugReproductionTests
{
   [Test]
   [Timeout(5000)]
   public async Task SocketConnection_ShouldRespectMaxReadBufferSize_ForReceiveBackpressureThreshold(CancellationToken ct)
   {
      var tcpOptions = new TcpTransportOptions
      {
         SocketOptions = new SocketTransportOptions
         {
            MaxReadBufferSize = 8192
         }
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), tcpOptions);
      await listener.BindAsync(ct);

      try
      {
         var client = new TcpNetworkClient(tcpOptions);
         var connectResult = await client.ConnectAsync(listener.LocalAddress, ct);
         var acceptResult = await listener.AcceptSessionAsync(ct);

         await using var clientSession = connectResult.Success!;
         await using var serverSession = acceptResult.Success!;

         // Inspect the underlying SocketReceiver Pipe's PauseWriterThreshold vs configured MaxReadBufferSize (8192)
         var connField = typeof(TcpNetworkSession).GetField("_connection", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
         var socketConn = connField.GetValue(serverSession)!;
         var receiverField = socketConn.GetType().GetField("_receiver", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
         var receiver = receiverField.GetValue(socketConn)!;
         var pipeProp = receiver.GetType().GetProperty("Pipe")!;
         var pipe = (Pipe)pipeProp.GetValue(receiver)!;
         var optionsField = typeof(Pipe).GetField("_options", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
         var pipeOptions = (PipeOptions)optionsField.GetValue(pipe)!;

         await Assert.That(pipeOptions.PauseWriterThreshold).IsEqualTo(8192L);
      }
      finally
      {
         await listener.UnbindAsync(CancellationToken.None);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task TcpSession_ShouldCancelSessionClosedToken_AndTriggerManagedClientDisconnect_WhenRemoteServerClosesSession(CancellationToken ct)
   {
      var tcpOptions = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), tcpOptions);
      await listener.BindAsync(ct);

      try
      {
         var innerClient = new TcpNetworkClient(tcpOptions);
         await using var managedClient = new NetworkClient(innerClient, new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 3,
            BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(50))
         });

         var disconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         managedClient.Disconnected.Add((_, _) =>
         {
            disconnectedTcs.TrySetResult();
            return ValueTask.CompletedTask;
         });

         var connectResult = await managedClient.ConnectAsync(listener.LocalAddress, ct);
         await Assert.That(connectResult.IsSuccess).IsTrue();

         var acceptResult = await listener.AcceptSessionAsync(ct);
         await Assert.That(acceptResult.Failed).IsFalse();
         var serverSession = acceptResult.Success!;

         // Ensure client stream is opened so SocketReceiver is active on client
         var clientStreamResult = await managedClient.Session!.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
         await Assert.That(clientStreamResult.Failed).IsFalse();

         // Remote server abruptly closes its session
         await serverSession.DisposeAsync();

         // Managed.NetworkClient relies on session.SessionClosedToken to detect remote disconnect
         var completed = await Task.WhenAny(disconnectedTcs.Task, Task.Delay(1500, ct));
         await Assert.That(completed == disconnectedTcs.Task).IsTrue();
      }
      finally
      {
         await listener.UnbindAsync(CancellationToken.None);
      }
   }

   [Test]
   public async Task NetworkPinnedBlockMemoryPool_DoubleDispose_ShouldNotReturnSameBlockToTwoCallers()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();

      var owner1 = pool.Rent();
      owner1.Dispose();

      var rentedA = pool.Rent();
      var rentedB = pool.Rent();

      try
      {
         var sameInstance = ReferenceEquals(rentedA, rentedB);
         await Assert.That(sameInstance).IsFalse();
      }
      finally
      {
         rentedA.Dispose();
         rentedB.Dispose();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task StreamConnection_StopAsync_ShouldFlushAndDrainPendingWrites_BeforeDisposingInnerStream(CancellationToken ct)
   {
      var trackingStream = new FlushTrackingStream();
      var streamConn = new StreamConnection(PipeOptions.Default, PipeOptions.Default);
      streamConn.Initialize(trackingStream, allowFlushOnStop: true);
      streamConn.Start();

      var payload = "Important final bytes before stop"u8.ToArray();
      await streamConn.Output.WriteAsync(payload, ct);

      // Immediately stop the connection
      await streamConn.StopAsync();

      await Assert.That(trackingStream.FlushCalled).IsTrue();
      await Assert.That(trackingStream.WrittenBytes).IsEquivalentTo(payload);
   }

   [Test]
   public async Task ExponentialBackoffPolicy_GetNextDelay_ShouldNotOverflowTimeSpanOrExceedTaskDelayLimit_WhenAttemptIs32OrGreater()
   {
      var policy = new ExponentialBackoffPolicy(TimeSpan.FromSeconds(10));

      await Assert.That(() =>
      {
         var delay = policy.GetNextDelay(32);
         using var cts = new CancellationTokenSource();
         cts.Cancel();
         _ = Task.Delay(delay, cts.Token);
      }).ThrowsNothing();
   }

   [Test]
   [Timeout(5000)]
   public async Task WsNetworkListener_WithOnMessageAsync_ShouldUpdateSessionStats(CancellationToken ct)
   {
      var messageReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      var serverOptions = new WsTransportOptions
      {
         OnMessageAsync = (_, _, _) =>
         {
            messageReceivedTcs.TrySetResult();
            return ValueTask.CompletedTask;
         }
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), serverOptions);
      await listener.BindAsync(ct);

      try
      {
         var client = new WsNetworkClient(new WsTransportOptions());
         var connectResult = await client.ConnectAsync(listener.LocalAddress, ct);
         await Assert.That(connectResult.Failed).IsFalse();

         await using var clientSession = (WsNetworkSession)connectResult.Success!;
         var payload = "Hello WebSocket"u8.ToArray();
         await clientSession.SendFrameAsync(payload, WebSocketOpcode.Text, flush: true, ct);

         var acceptResult = await listener.AcceptSessionAsync(ct);
         await Assert.That(acceptResult.Failed).IsFalse();
         await using var serverSession = (WsNetworkSession)acceptResult.Success!;

         await messageReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);

         await Assert.That(clientSession.Stats.BytesSent > 0).IsTrue();
         await Assert.That(serverSession.Stats.BytesReceived > 0).IsTrue();
      }
      finally
      {
         await listener.UnbindAsync(CancellationToken.None);
      }
   }

   [Test]
   public async Task TraceLogger_LogWithBracesAndNoFormatArgs_ShouldNotThrowFormatException()
   {
      var prevEnabled = TraceLogger.IsEnabled;
      TraceLogger.IsEnabled = true;

      try
      {
         await Assert.That(() => TraceLogger.LogNeutralInfo("JSON payload: {\"status\": \"ok\"}")).ThrowsNothing();
      }
      finally
      {
         TraceLogger.IsEnabled = prevEnabled;
      }
   }

   [Test]
   public async Task ConsoleRender_WriteMarkup_WithBracketedNumbers_ShouldNotThrowArgumentException()
   {
      await Assert.That(() => ConsoleRender.WriteMarkup("Item [16] and index [0]")).ThrowsNothing();
   }

   private sealed class FlushTrackingStream : MemoryStream
   {
      private readonly MemoryStream _captured = new();
      public bool FlushCalled { get; private set; }
      public byte[] WrittenBytes => _captured.ToArray();

      public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
      {
         // Simulate slight async write latency so StopAsync races with CopyWritePipeToStream
         await Task.Delay(30, cancellationToken);
         await _captured.WriteAsync(buffer, cancellationToken);
      }

      public override Task FlushAsync(CancellationToken cancellationToken)
      {
         FlushCalled = true;
         return Task.CompletedTask;
      }
   }
}

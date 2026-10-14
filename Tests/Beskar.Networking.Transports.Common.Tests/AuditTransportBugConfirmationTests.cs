using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Interfaces.Misc;
using Beskar.Networking.Abstractions.Managed;
using Beskar.Networking.Abstractions.Managed.Events;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Options;
using Beskar.Networking.Transports.Common.Pipelines;
using Beskar.Networking.Transports.Common.Sockets;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;
using Beskar.Networking.Transports.Ws.Enums;

namespace Beskar.Networking.Transports.Common.Tests;

[NotInParallel]
public class AuditTransportBugConfirmation
{
   [Test]
   [Timeout(5000)]
   public async Task SocketReceiver_Stop_WhileConsumerHoldsReadBuffer_ShouldNotRecyclePinnedBlockOrCorruptData(CancellationToken ct)
   {
      using var pool = new NetworkPinnedBlockMemoryPool();
      var pipeOptions = new PipeOptions(
         pool: pool,
         readerScheduler: PipeScheduler.Inline,
         writerScheduler: PipeScheduler.Inline,
         useSynchronizationContext: false);

      var receiver = new SocketReceiver(pipeOptions);

      // Simulate 4 bytes received into the PipeWriter
      var expectedBytes = new byte[] { 0x11, 0x22, 0x33, 0x44 };
      var span = receiver.Pipe.Writer.GetSpan(expectedBytes.Length);
      expectedBytes.CopyTo(span);
      receiver.Pipe.Writer.Advance(expectedBytes.Length);
      await receiver.Pipe.Writer.FlushAsync(ct);

      // Consumer reads the buffer and is actively inspecting/parsing it (before AdvanceTo)
      var readResult = await receiver.Pipe.Reader.ReadAsync(ct);
      var consumerMemory = readResult.Buffer.First;
      await Assert.That(consumerMemory.Span[..4].SequenceEqual(expectedBytes)).IsTrue();
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(1);

      // SocketConnection.Abort() / SocketReceiver.Stop() runs concurrently
      receiver.Stop();

      // Another connection rents a block from the same pool and writes 0xFF into it
      using var anotherConnectionBlock = pool.Rent();
      anotherConnectionBlock.Memory.Span.Fill(0xFF);

      // Expected: The consumer's unread buffer must NOT have been returned to the pool and overwritten
      // before the consumer calls AdvanceTo / Complete!
      await Assert.That(consumerMemory.Span[..4].SequenceEqual(expectedBytes)).IsTrue();
   }

   [Test]
   [Timeout(5000)]
   public async Task TryResetState_AfterDisposeAsync_ShouldNotThrowObjectDisposedException(CancellationToken ct)
   {
      var receiver = new SocketReceiver(PipeOptions.Default);
      await receiver.DisposeAsync();

      var sender = new SocketSender(PipeOptions.Default);
      await sender.DisposeAsync();

      var streamConnection = new StreamConnection(PipeOptions.Default, PipeOptions.Default);
      await streamConnection.DisposeAsync();

      // Expected: TryResetState() on a disposed pooled object should either return false cleanly
      // or reset without throwing ObjectDisposedException.
      var receiverReset = receiver.TryResetState();
      var senderReset = sender.TryResetState();
      var streamReset = streamConnection.TryResetState();

      await Assert.That(receiverReset || !receiverReset).IsTrue();
      await Assert.That(senderReset || !senderReset).IsTrue();
      await Assert.That(streamReset || !streamReset).IsTrue();
   }

   [Test]
   [Timeout(5000)]
   public async Task WsDuplexPipe_DataFrameCoalescedWithCloseFrame_ShouldNotThrowInFinallyOrSkipReaderAdvanceTo(CancellationToken ct)
   {
      var clientToServer = new Pipe();
      var serverToClient = new Pipe();
      var trackingReader = new AdvanceTrackingPipeReader(clientToServer.Reader);
      var rawTransport = new DuplexPipePair(trackingReader, serverToClient.Writer);
      var fakeTcpSession = new FakeTransportSession();

      await using var wsPipe = new WsDuplexPipe(
         rawTransport,
         fakeTcpSession,
         maskOutgoing: false,
         options: new WsTransportOptions());

      // Write a masked Binary frame (FIN=1, Opcode=2, MASK=1, Len=4, MaskKey=0,0,0,0, Payload=0xDE,0xAD,0xBE,0xEF)
      // AND a masked Close frame (FIN=1, Opcode=8, MASK=1, Len=0, MaskKey=0,0,0,0)
      // into clientToServer.Writer BEFORE flushing so they arrive in a single ReadAsync result.
      var payload = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
      clientToServer.Writer.Write(new byte[]
      {
         0x82, 0x84, 0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF,
         0x88, 0x80, 0x00, 0x00, 0x00, 0x00
      });
      await clientToServer.Writer.FlushAsync(ct);

      var completedReadResult = await wsPipe.Input.ReadAsync(ct);
      wsPipe.Input.AdvanceTo(completedReadResult.Buffer.End);
      await trackingReader.CompletedTcs.Task.WaitAsync(ct);

      await Assert.That(trackingReader.CompletedWithUnadvancedRead).IsFalse();
   }

   [Test]
   [Timeout(5000)]
   public async Task TcpNetworkListener_UnbindDuringTlsHandshake_ShouldNotThrowObjectDisposedExceptionOnSemaphoreRelease(CancellationToken ct)
   {
      using var cert = System.Security.Cryptography.X509Certificates.X509CertificateLoader.LoadPkcs12(
          CreateSelfSignedPfx(), null);

      var options = new TcpTransportOptions
      {
         UseSsl = true,
         SslServerOptions = new System.Net.Security.SslServerAuthenticationOptions
         {
            ServerCertificate = cert
         },
         MaxConcurrentHandshakes = 1
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindRes = await listener.BindAsync(ct);
      await Assert.That(bindRes.IsSuccess).IsTrue();

      // Connect a raw TCP socket that sends NO TLS ClientHello so HandshakeAndEnqueueAsync blocks in AuthenticateAsServerAsync
      using var rawClient = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      await rawClient.ConnectAsync(listener.LocalAddress, ct);

      // Wait briefly until AcceptLoopAsync has acquired _handshakeSemaphore and started HandshakeAndEnqueueAsync
      await Task.Delay(100, ct);

      // Capture FirstChanceException for ObjectDisposedException from SemaphoreSlim.Release()
      ObjectDisposedException? semaphoreOde = null;
      void OnFirstChance(object? sender, System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs e)
      {
         if (e.Exception is ObjectDisposedException ode &&
             ode.ObjectName == typeof(SemaphoreSlim).FullName)
         {
            semaphoreOde = ode;
         }
      }

      AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;
      try
      {
         await listener.UnbindAsync(ct);
         await Task.Delay(150, ct);
      }
      finally
      {
         AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance;
         await listener.DisposeAsync();
      }

      await Assert.That(semaphoreOde).IsNull();
   }

   private static byte[] CreateSelfSignedPfx()
   {
      using var rsa = System.Security.Cryptography.RSA.Create(2048);
      var req = new System.Security.Cryptography.X509Certificates.CertificateRequest(
         "CN=localhost",
         rsa,
         System.Security.Cryptography.HashAlgorithmName.SHA256,
         System.Security.Cryptography.RSASignaturePadding.Pkcs1);
      using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
      return cert.Export(System.Security.Cryptography.X509Certificates.X509ContentType.Pfx);
   }

   private sealed class DuplexPipePair(PipeReader input, PipeWriter output) : IDuplexPipe
   {
      public PipeReader Input { get; } = input;
      public PipeWriter Output { get; } = output;
   }

   private sealed class AdvanceTrackingPipeReader(PipeReader inner) : PipeReader
   {
      private bool _readInProgress;
      public bool CompletedWithUnadvancedRead { get; private set; }
      public TaskCompletionSource CompletedTcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

      public override bool TryRead(out ReadResult result)
      {
         var ok = inner.TryRead(out result);
         if (ok) _readInProgress = true;
         return ok;
      }

      public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
      {
         var res = await inner.ReadAsync(cancellationToken);
         _readInProgress = true;
         return res;
      }

      public override void AdvanceTo(SequencePosition consumed)
      {
         _readInProgress = false;
         inner.AdvanceTo(consumed);
      }

      public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
      {
         _readInProgress = false;
         inner.AdvanceTo(consumed, examined);
      }

      public override void CancelPendingRead() => inner.CancelPendingRead();

      public override void Complete(Exception? exception = null)
      {
         if (_readInProgress) CompletedWithUnadvancedRead = true;
         inner.Complete(exception);
         CompletedTcs.TrySetResult();
      }

      public override async ValueTask CompleteAsync(Exception? exception = null)
      {
         if (_readInProgress) CompletedWithUnadvancedRead = true;
         await inner.CompleteAsync(exception);
         CompletedTcs.TrySetResult();
      }
   }

   private sealed class ConstantBackoffPolicy(TimeSpan delay) : IBackoffPolicy
   {
      public TimeSpan GetNextDelay(int attemptIndex) => delay;
   }

   private sealed class FakeTransportSession : INetworkSession
   {
      private readonly CancellationTokenSource _cts = new();

      public Guid Id { get; } = Guid.CreateVersion7();
      public EndPoint RemoteAddress { get; } = new IPEndPoint(IPAddress.Loopback, 12345);
      public EndPoint LocalAddress { get; } = new IPEndPoint(IPAddress.Loopback, 54321);
      public bool IsSupportingMultiplexing => false;
      public bool IsSupportingUnidirectional => false;
      public CancellationToken SessionClosedToken => _cts.Token;
      public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();
      public NetworkStats Stats => new();
      public NetworkSessionStats SessionStats => new();
      public IReadOnlyCollection<INetworkStream> ActiveStreams => [];
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public TransportKind Transport => TransportKind.Tcp;
      public NetworkSecurityInfo SecurityInfo => new(false, null, null, null, null);

      public void TriggerClose() => _cts.Cancel();

      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
         CancellationToken ct = default) =>
         throw new NotSupportedException();

      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default) =>
         throw new NotSupportedException();

      public ValueTask DisposeAsync()
      {
         try { _cts.Cancel(); } catch { }
         _cts.Dispose();
         return ValueTask.CompletedTask;
      }
   }

   private sealed class FakeSlowConnectNetworkClient(
      Func<CancellationToken, ValueTask<Result<INetworkSession, NetworkCodeError>>> onFirstConnectAsync,
      Func<CancellationToken, ValueTask<Result<INetworkSession, NetworkCodeError>>> onSubsequentConnectAsync) : INetworkClient
   {
      private int _calls;
      public TransportKind Transport => TransportKind.Tcp;
      public bool IsConnected => false;
      public INetworkSession? Session => null;
      public EndPoint? LocalAddress => null;
      public EndPoint? RemoteAddress => null;
      public NetworkClientStats Stats => new();

      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default)
      {
         return Interlocked.Increment(ref _calls) == 1
            ? onFirstConnectAsync(ct)
            : onSubsequentConnectAsync(ct);
      }

      public ValueTask DisconnectAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default) =>
         throw new NotSupportedException();

      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default) =>
         throw new NotSupportedException();

      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private sealed class FakeCallbackNetworkClient(
      Func<CancellationToken, ValueTask<Result<INetworkSession, NetworkCodeError>>> onConnectAsync) : INetworkClient
   {
      public TransportKind Transport => TransportKind.Tcp;
      public bool IsConnected => false;
      public INetworkSession? Session => null;
      public EndPoint? LocalAddress => null;
      public EndPoint? RemoteAddress => null;
      public NetworkClientStats Stats => new();

      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default) =>
         onConnectAsync(ct);

      public ValueTask DisconnectAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default) =>
         throw new NotSupportedException();

      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default) =>
         throw new NotSupportedException();

      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }
}

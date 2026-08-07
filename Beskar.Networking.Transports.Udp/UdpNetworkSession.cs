using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Udp;

/// <summary>
/// Represents an active UDP network session.
/// </summary>
public sealed class UdpNetworkSession : INetworkSession
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress { get; }
   public EndPoint LocalAddress { get; }

   public bool IsSupportingMultiplexing => false;
   public bool IsSupportingUnidirectional => false;

   private readonly CancellationToken _sessionClosedToken;
   public CancellationToken SessionClosedToken => _sessionClosedToken;

   public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();

   public NetworkStats Stats => _stream?.Stats ?? new NetworkStats();

   private long _streamsAccepted;
   private long _streamsOpened;

   public NetworkSessionStats SessionStats => new()
   {
      StreamsAccepted = Interlocked.Read(ref _streamsAccepted),
      StreamsOpened = Interlocked.Read(ref _streamsOpened)
   };

   public IReadOnlyCollection<INetworkStream> ActiveStreams => [_stream];

   public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
   public TransportKind Transport => TransportKind.Udp;

   public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);

   private readonly CancellationTokenSource _cts = new();
   private readonly Pipe _incomingPipe;
   private readonly Pipe _outgoingPipe;

   private readonly UdpNetworkStream _stream;

   private readonly Socket? _clientSocket; // null for server-side
   private readonly Func<ReadOnlyMemory<byte>, EndPoint, ValueTask>? _sendToFunc; // null for client-side
   private readonly Func<UdpNetworkSession, ValueTask>? _onDisposeAsync;

   private readonly Task _sendLoopTask;
   private readonly Task? _receiveLoopTask;

   private long _lastActivityTicks;
   public long LastActivityTicks => Volatile.Read(ref _lastActivityTicks);

   private int _isWriterPaused;
   private int _disposed;

   public UdpNetworkSession(
      Socket socket,
      EndPoint localAddress,
      EndPoint remoteAddress,
      UdpTransportOptions options,
      Func<UdpNetworkSession, ValueTask>? onDisposeAsync = null)
   {
      _clientSocket = socket;
      LocalAddress = localAddress;
      RemoteAddress = remoteAddress;
      _onDisposeAsync = onDisposeAsync;
      _sessionClosedToken = _cts.Token;

      _incomingPipe = new Pipe(new PipeOptions(
         pauseWriterThreshold: options.IncomingPipePauseThreshold,
         resumeWriterThreshold: options.IncomingPipeResumeThreshold,
         useSynchronizationContext: false));

      _outgoingPipe = new Pipe(new PipeOptions(
         pauseWriterThreshold: options.OutgoingPipePauseThreshold,
         resumeWriterThreshold: options.OutgoingPipeResumeThreshold,
         useSynchronizationContext: false));

      IDuplexPipe sessionPipe = new DuplexPipe(_incomingPipe.Reader, _outgoingPipe.Writer);
      _stream = new UdpNetworkStream(this, sessionPipe);

      Volatile.Write(ref _lastActivityTicks, DateTimeOffset.UtcNow.Ticks);
      TransportMetrics.RecordConnectionOpened(TransportKind.Udp);

      _sendLoopTask = Task.Run(() => ProcessSendLoopClientAsync(options.MaxPacketSize));
      _receiveLoopTask = Task.Run(ProcessReceiveLoopClientAsync);
   }

   public UdpNetworkSession(
      EndPoint localAddress,
      EndPoint remoteAddress,
      Func<ReadOnlyMemory<byte>, EndPoint, ValueTask> sendToFunc,
      UdpTransportOptions options,
      Func<UdpNetworkSession, ValueTask>? onDisposeAsync = null)
   {
      LocalAddress = localAddress;
      RemoteAddress = remoteAddress;
      _sendToFunc = sendToFunc;
      _onDisposeAsync = onDisposeAsync;
      _sessionClosedToken = _cts.Token;

      _incomingPipe = new Pipe(new PipeOptions(
         pauseWriterThreshold: options.IncomingPipePauseThreshold,
         resumeWriterThreshold: options.IncomingPipeResumeThreshold,
         useSynchronizationContext: false));

      _outgoingPipe = new Pipe(new PipeOptions(
         pauseWriterThreshold: options.OutgoingPipePauseThreshold,
         resumeWriterThreshold: options.OutgoingPipeResumeThreshold,
         useSynchronizationContext: false));

      IDuplexPipe sessionPipe = new DuplexPipe(_incomingPipe.Reader, _outgoingPipe.Writer);
      _stream = new UdpNetworkStream(this, sessionPipe);

      Volatile.Write(ref _lastActivityTicks, DateTimeOffset.UtcNow.Ticks);
      TransportMetrics.RecordConnectionOpened(TransportKind.Udp);

      _sendLoopTask = Task.Run(() => ProcessSendLoopServerAsync(options.MaxPacketSize));
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
   {
      Interlocked.Increment(ref _streamsAccepted);
      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      Interlocked.Increment(ref _streamsOpened);
      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public ValueTask PushIncomingDataAsync(ReadOnlyMemory<byte> data)
   {
      Volatile.Write(ref _lastActivityTicks, DateTimeOffset.UtcNow.Ticks);

      if (Volatile.Read(ref _isWriterPaused) == 1)
      {
         // Drop packet since the session pipe is full
         return ValueTask.CompletedTask;
      }

      var writer = _incomingPipe.Writer;
      writer.Write(data.Span);

      var flushTask = writer.FlushAsync(_cts.Token);
      if (!flushTask.IsCompleted)
      {
         Volatile.Write(ref _isWriterPaused, 1);
         _ = ObserveFlushAsync(flushTask);
      }

      return ValueTask.CompletedTask;

      async Task ObserveFlushAsync(ValueTask<FlushResult> task)
      {
         try
         {
            await task;
         }
         catch
         {
            // Ignored
         }
         finally
         {
            Volatile.Write(ref _isWriterPaused, 0);
         }
      }
   }

   private async Task ProcessReceiveLoopClientAsync()
   {
      var socket = _clientSocket;
      if (socket is null) return;

      var writer = _incomingPipe.Writer;

      try
      {
         var buffer = new byte[65536];
         while (!_cts.Token.IsCancellationRequested)
         {
            var bytesReceived = await socket.ReceiveAsync(buffer, SocketFlags.None, _cts.Token);
            if (bytesReceived == 0)
            {
               continue;
            }

            Volatile.Write(ref _lastActivityTicks, DateTimeOffset.UtcNow.Ticks);

            writer.Write(buffer.AsSpan(0, bytesReceived));
            var flushResult = await writer.FlushAsync(_cts.Token);

            if (flushResult.IsCompleted || flushResult.IsCanceled)
            {
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("UDP Session {0}: Receive loop encountered error: {1}", Id, ex.Message);
      }
      finally
      {
         await writer.CompleteAsync();

         try
         {
            await _cts.CancelAsync();
         }
         catch (ObjectDisposedException)
         {
            // Already disposed
         }
      }
   }

   private async Task ProcessSendLoopClientAsync(int maxPacketSize)
   {
      var socket = _clientSocket;
      if (socket is null) return;

      var reader = _outgoingPipe.Reader;

      try
      {
         while (!_cts.Token.IsCancellationRequested)
         {
            var readResult = await reader.ReadAsync(_cts.Token);
            var buffer = readResult.Buffer;

            if ((buffer.IsEmpty && readResult.IsCompleted) || readResult.IsCanceled)
            {
               break;
            }

            if (!buffer.IsEmpty)
            {
               await SendBufferClientAsync(socket, buffer, maxPacketSize);
            }

            reader.AdvanceTo(buffer.End);

            if (readResult.IsCompleted)
            {
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("UDP Session {0}: Send loop encountered error: {1}", Id, ex.Message);
      }
      finally
      {
         await reader.CompleteAsync();

         try
         {
            await _cts.CancelAsync();
         }
         catch (ObjectDisposedException)
         {
            // Already disposed
         }
      }
   }

   private async Task ProcessSendLoopServerAsync(int maxPacketSize)
   {
      var sendTo = _sendToFunc;
      if (sendTo is null) return;

      var reader = _outgoingPipe.Reader;

      try
      {
         while (!_cts.Token.IsCancellationRequested)
         {
            var readResult = await reader.ReadAsync(_cts.Token);
            var buffer = readResult.Buffer;

            if ((buffer.IsEmpty && readResult.IsCompleted) || readResult.IsCanceled)
            {
               break;
            }

            if (!buffer.IsEmpty)
            {
               await SendBufferServerAsync(sendTo, buffer, maxPacketSize);
            }

            reader.AdvanceTo(buffer.End);

            if (readResult.IsCompleted)
            {
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("UDP Session {0}: Send loop encountered error: {1}", Id, ex.Message);
      }
      finally
      {
         await reader.CompleteAsync();

         try
         {
            await _cts.CancelAsync();
         }
         catch (ObjectDisposedException)
         {
            // Already disposed
         }
      }
   }

   private async ValueTask SendBufferClientAsync(Socket socket, ReadOnlySequence<byte> buffer, int maxPacketSize)
   {
      var remaining = buffer;
      while (!remaining.IsEmpty)
      {
         var chunkSize = (int)Math.Min(remaining.Length, maxPacketSize);
         var chunk = remaining.Slice(0, chunkSize);

         byte[]? rented = null;
         ReadOnlyMemory<byte> memoryToSend;
         if (chunk.IsSingleSegment)
         {
            memoryToSend = chunk.First;
         }
         else
         {
            rented = ArrayPool<byte>.Shared.Rent(chunkSize);
            chunk.CopyTo(rented);
            memoryToSend = rented.AsMemory(0, chunkSize);
         }

         try
         {
            await socket.SendAsync(memoryToSend, SocketFlags.None, _cts.Token);
            Volatile.Write(ref _lastActivityTicks, DateTimeOffset.UtcNow.Ticks);
         }
         finally
         {
            if (rented is not null)
            {
               ArrayPool<byte>.Shared.Return(rented);
            }
         }

         remaining = remaining.Slice(chunkSize);
      }
   }

   private async ValueTask SendBufferServerAsync(Func<ReadOnlyMemory<byte>, EndPoint, ValueTask> sendTo, ReadOnlySequence<byte> buffer, int maxPacketSize)
   {
      var remaining = buffer;
      while (!remaining.IsEmpty)
      {
         var chunkSize = (int)Math.Min(remaining.Length, maxPacketSize);
         var chunk = remaining.Slice(0, chunkSize);

         byte[]? rented = null;
         ReadOnlyMemory<byte> memoryToSend;
         if (chunk.IsSingleSegment)
         {
            memoryToSend = chunk.First;
         }
         else
         {
            rented = ArrayPool<byte>.Shared.Rent(chunkSize);
            chunk.CopyTo(rented);
            memoryToSend = rented.AsMemory(0, chunkSize);
         }

         try
         {
            await sendTo(memoryToSend, RemoteAddress);
            Volatile.Write(ref _lastActivityTicks, DateTimeOffset.UtcNow.Ticks);
         }
         finally
         {
            if (rented is not null)
            {
               ArrayPool<byte>.Shared.Return(rented);
            }
         }

         remaining = remaining.Slice(chunkSize);
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      TransportMetrics.RecordConnectionClosed(TransportKind.Udp);

      TraceLogger.LogNeutralInfo("UDP Session: Disposing active UDP session {0}", Id);

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }
      _cts.Dispose();

      await _incomingPipe.Reader.CompleteAsync();
      await _incomingPipe.Writer.CompleteAsync();
      await _outgoingPipe.Reader.CompleteAsync();
      await _outgoingPipe.Writer.CompleteAsync();

      try
      {
         await _sendLoopTask;
      }
      catch
      {
         // Ignored
      }

      if (_receiveLoopTask is not null)
      {
         try
         {
            await _receiveLoopTask;
         }
         catch
         {
            // Ignored
         }
      }

      if (_clientSocket is not null)
      {
         try
         {
            _clientSocket.Dispose();
         }
         catch
         {
            // Ignored
         }
      }

      if (_onDisposeAsync is not null)
      {
         await _onDisposeAsync(this);
      }
   }

   private sealed class DuplexPipe(PipeReader reader, PipeWriter writer) : IDuplexPipe
   {
      public PipeReader Input => reader;
      public PipeWriter Output => writer;
   }
}

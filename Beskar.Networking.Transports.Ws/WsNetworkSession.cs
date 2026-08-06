using System.Buffers;
using System.Net;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using System.IO.Pipelines;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Transports.Ws.Enums;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// Represents an active WebSocket network session.
/// </summary>
public sealed class WsNetworkSession : INetworkSession
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress => _tcpSession.RemoteAddress;
   public EndPoint LocalAddress => _tcpSession.LocalAddress;

   public bool IsSupportingMultiplexing => false;
   public bool IsSupportingUnidirectional => false;

   public CancellationToken SessionClosedToken => _cts.Token;

   public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();

   public NetworkStats Stats => _stream.Stats;

   private long _streamsAccepted;
   private long _streamsOpened;

   public NetworkSessionStats SessionStats => new()
   {
      StreamsAccepted = Interlocked.Read(ref _streamsAccepted),
      StreamsOpened = Interlocked.Read(ref _streamsOpened)
   };

   public IReadOnlyCollection<INetworkStream> ActiveStreams => [_stream];

   public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
   public TransportKind Transport => TransportKind.WebSocket;

   public NetworkSecurityInfo SecurityInfo => _tcpSession.SecurityInfo;

   private readonly INetworkSession _tcpSession;
   private readonly IDuplexPipe _wsPipe;

   private readonly WsNetworkStream _stream;
   private readonly CancellationTokenSource _cts = new();

   private int _disposed;

   public WsNetworkSession(INetworkSession tcpSession, IDuplexPipe wsPipe)
   {
      _tcpSession = tcpSession;
      _wsPipe = wsPipe;

      _stream = new WsNetworkStream(this, wsPipe);

      if (_wsPipe is WsDuplexPipe wsDuplexPipe)
      {
         wsDuplexPipe.SetSession(this);
      }

      _tcpSession.SessionClosedToken.Register(() =>
      {
         try
         {
            _cts.Cancel();
         }
         catch
         {
            // Ignored
         }
      });
   }

   public ValueTask SendFrameAsync(ReadOnlySequence<byte> payload, 
      WebSocketOpcode opcode = WebSocketOpcode.Binary, CancellationToken ct = default)
   {
      return _stream.SendFrameAsync(payload, opcode, ct);
   }

   public ValueTask SendFrameAsync(ReadOnlyMemory<byte> payload, 
      WebSocketOpcode opcode = WebSocketOpcode.Binary, CancellationToken ct = default)
   {
      return _stream.SendFrameAsync(payload, opcode, ct);
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
   {
      TraceLogger.LogServerInfo("WS Session {0}: Instantiating WebSocket stream wrapper", Id);
      Interlocked.Increment(ref _streamsAccepted);
      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("WS Session {0}: Opening WebSocket stream wrapper", Id);
      Interlocked.Increment(ref _streamsOpened);
      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      TraceLogger.LogNeutralInfo("WS Session: Disposing and shutting down active WebSocket session {0} (Remote: {1}, Local: {2})", Id, RemoteAddress, LocalAddress);

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }
      _cts.Dispose();

      await _stream.DisposeAsync();

      if (_wsPipe is IAsyncDisposable wsAsyncDisposable)
      {
         await wsAsyncDisposable.DisposeAsync();
      }

      if (_tcpSession is IAsyncDisposable asyncDisposable)
      {
         await asyncDisposable.DisposeAsync();
      }
   }
}

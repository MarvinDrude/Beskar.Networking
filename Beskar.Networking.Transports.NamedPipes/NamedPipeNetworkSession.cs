using System.IO.Pipelines;
using System.Net;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.NamedPipes;

/// <summary>
/// Represents a Named Pipe network session wrapping a single transport connection.
/// </summary>
public sealed class NamedPipeNetworkSession : INetworkSession
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress { get; }
   public EndPoint LocalAddress { get; }

   public bool IsSupportingMultiplexing => false;
   public bool IsSupportingUnidirectional => false;

   public CancellationToken SessionClosedToken => _cts.Token;

   public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();

   public NetworkStats Stats => _stream?.Stats ?? new NetworkStats();

   private long _streamsAccepted;
   private long _streamsOpened;

   public NetworkSessionStats SessionStats => new()
   {
      StreamsAccepted = Interlocked.Read(ref _streamsAccepted),
      StreamsOpened = Interlocked.Read(ref _streamsOpened)
   };

   public IReadOnlyCollection<INetworkStream> ActiveStreams => _stream is not null
      ? new[] { _stream }
      : Array.Empty<INetworkStream>();

   public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
   public TransportKind Transport => TransportKind.NamedPipe;

   public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);

   private readonly IDuplexPipe _connection;
   private readonly Func<IDuplexPipe, ValueTask>? _onDisposeAsync;
   private readonly CancellationTokenSource _cts = new();

   private NamedPipeNetworkStream? _stream;
   private int _disposed;

   public NamedPipeNetworkSession(
      EndPoint localAddress,
      EndPoint remoteAddress,
      IDuplexPipe connection,
      Func<IDuplexPipe, ValueTask>? onDisposeAsync = null)
   {
      LocalAddress = localAddress;
      RemoteAddress = remoteAddress;
      _connection = connection;
      _onDisposeAsync = onDisposeAsync;
      TransportMetrics.RecordConnectionOpened(TransportKind.NamedPipe);
   }

   /// <inheritdoc />
   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(
      CancellationToken ct = default)
   {
      if (Volatile.Read(ref _disposed) == 1)
      {
         throw new ObjectDisposedException(nameof(NamedPipeNetworkSession));
      }

      if (_stream is null)
      {
         TraceLogger.LogServerInfo("NamedPipe Session {0}: Instantiating Named Pipe stream wrapper", Id);
         _stream = new NamedPipeNetworkStream(this, _connection);
         Interlocked.Increment(ref _streamsAccepted);
      }

      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      if (Volatile.Read(ref _disposed) == 1)
      {
         throw new ObjectDisposedException(nameof(NamedPipeNetworkSession));
      }

      if (_stream is null)
      {
         TraceLogger.LogClientInfo("NamedPipe Session {0}: Opening Named Pipe stream connection", Id);
         _stream = new NamedPipeNetworkStream(this, _connection);
         Interlocked.Increment(ref _streamsOpened);
      }

      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      TransportMetrics.RecordConnectionClosed(TransportKind.NamedPipe);
      var origin = _onDisposeAsync is not null ? TraceLogOrigin.Server : TraceLogOrigin.Client;
      TraceLogger.LogInfo("Disposing and shutting down active Named Pipe session {0}", origin, Id);

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }

      _cts.Dispose();

      if (_stream is not null)
      {
         try
         {
            await _stream.DisposeAsync();
         }
         catch
         {
            // Ignored
         }
      }

      if (_onDisposeAsync is not null)
      {
         await _onDisposeAsync(_connection);
      }
   }
}

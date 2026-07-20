using System.IO.Pipelines;
using System.Net;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Memory;

/// <summary>
/// An in-memory implementation of <see cref="INetworkSession"/>.
/// </summary>
public sealed class MemoryNetworkSession : INetworkSession
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
   public TransportKind Transport => TransportKind.Memory;

   public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);

   private readonly IDuplexPipe _connection;
   private readonly CancellationTokenSource _cts = new();

   private MemoryNetworkStream? _stream;
   private MemoryNetworkSession? _peerSession;
   private int _disposed;

   public MemoryNetworkSession(EndPoint localAddress, EndPoint remoteAddress, IDuplexPipe connection)
   {
      LocalAddress = localAddress;
      RemoteAddress = remoteAddress;
      _connection = connection;
   }

   internal void SetPeer(MemoryNetworkSession peer)
   {
      _peerSession = peer;
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(
      CancellationToken ct = default)
   {
      if (_stream is null)
      {
         TraceLogger.LogServerInfo("Memory Session {0}: Instantiating bidirectional stream wrapper", Id);
         _stream = new MemoryNetworkStream(this, _connection);
         Interlocked.Increment(ref _streamsAccepted);
      }

      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      if (_stream is null)
      {
         TraceLogger.LogClientInfo("Memory Session {0}: Opening bidirectional stream", Id);
         _stream = new MemoryNetworkStream(this, _connection);
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

      TraceLogger.LogInfo("Disposing active Memory session {0}", TraceLogOrigin.None, Id);

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

      try
      {
         await _connection.Input.CompleteAsync();
      }
      catch
      {
         // Ignored
      }

      try
      {
         await _connection.Output.CompleteAsync();
      }
      catch
      {
         // Ignored
      }

      var peer = _peerSession;
      if (peer is not null)
      {
         _peerSession = null;
         await peer.DisposeAsync();
      }
   }
}

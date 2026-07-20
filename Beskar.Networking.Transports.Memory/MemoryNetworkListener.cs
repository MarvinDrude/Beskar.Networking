using System.Net;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Memory;

/// <summary>
/// An in-memory implementation of <see cref="INetworkListener"/>.
/// </summary>
public sealed class MemoryNetworkListener(
   MemoryEndPoint localAddress,
   MemoryTransportOptions options)
   : INetworkListener
{
   private readonly MemoryEndPoint _configuredLocalAddress = localAddress;
   public EndPoint LocalAddress => _configuredLocalAddress;

   public TransportKind Transport => TransportKind.Memory;
   public bool IsBound => _isBound;

   private bool _isBound;
   private long _binds;
   private long _unbinds;
   private long _sessionsAccepted;

   public NetworkListenerStats Stats => new()
   {
      Binds = Interlocked.Read(ref _binds),
      Unbinds = Interlocked.Read(ref _unbinds),
      SessionsAccepted = Interlocked.Read(ref _sessionsAccepted)
   };

   private readonly MemoryTransportOptions _options = options;
   private Channel<Result<INetworkSession, NetworkCodeError>>? _sessionChannel;
   private bool _disposed;

   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      if (_isBound)
      {
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, "Listener is already bound."));
      }

      _sessionChannel = Channel.CreateBounded<Result<INetworkSession, NetworkCodeError>>(
         new BoundedChannelOptions(_options.MaxPendingConnections)
         {
            SingleWriter = false,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
         });

      TraceLogger.LogServerInfo("Memory Listener: Binding to address {0}", LocalAddress);
      if (!MemoryTransportRegistry.TryRegister(_configuredLocalAddress.Address, this))
      {
         TraceLogger.LogServerError("Memory Listener: Failed to bind to {0}. Address already in use.", LocalAddress);
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, $"Address '{_configuredLocalAddress.Address}' is already bound by another listener."));
      }

      _isBound = true;
      Interlocked.Increment(ref _binds);
      TraceLogger.LogServerInfo("Memory Listener: Successfully bound and listening on {0}", LocalAddress);

      return new ValueTask<VoidResult<NetworkCodeError>>(true);
   }

   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      if (!_isBound)
      {
         return true;
      }

      TraceLogger.LogServerInfo("Memory Listener: Unbinding and stopping listener on {0}", LocalAddress);
      _isBound = false;
      MemoryTransportRegistry.TryUnregister(_configuredLocalAddress.Address, this);

      var channel = _sessionChannel;
      if (channel is not null)
      {
         channel.Writer.TryComplete();
         while (channel.Reader.TryRead(out var result))
         {
            if (!result.Failed)
            {
               await result.Success.DisposeAsync();
            }
         }
      }

      Interlocked.Increment(ref _unbinds);
      TraceLogger.LogServerInfo("Memory Listener: Successfully unbound from {0}", LocalAddress);

      return true;
   }

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      if (!_isBound || _sessionChannel is null)
      {
         return new NetworkCodeError(-1, "Listener is not bound. Call BindAsync first.");
      }

      try
      {
         return _sessionChannel.Reader.TryRead(out var result)
            ? result
            : await _sessionChannel.Reader.ReadAsync(ct);
      }
      catch (ChannelClosedException)
      {
         return new NetworkCodeError(-1, "Listener has been unbound and session channel is closed.");
      }
   }

   internal async ValueTask<bool> TryEnqueueSessionAsync(MemoryNetworkSession session, CancellationToken ct)
   {
      if (!_isBound || _sessionChannel is null) return false;
      try
      {
         await _sessionChannel.Writer.WriteAsync(session, ct);
         Interlocked.Increment(ref _sessionsAccepted);

         TraceLogger.LogServerInfo("Memory Listener: Enqueued network session {0}", session.Id);
         return true;
      }
      catch
      {
         return false;
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      await UnbindAsync();
   }
}

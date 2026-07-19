using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Udp;

/// <summary>
/// A high-performance UDP client.
/// </summary>
public sealed class UdpNetworkClient(UdpTransportOptions options) : INetworkClient
{
   public TransportKind Transport => TransportKind.Udp;

   [MemberNotNullWhen(true, nameof(_activeSession), nameof(Session))]
   public bool IsConnected => _activeSession is not null
      && !_activeSession.SessionClosedToken.IsCancellationRequested;

   public INetworkSession? Session => _activeSession;

   public EndPoint? LocalAddress => _activeSession?.LocalAddress;
   public EndPoint? RemoteAddress => _activeSession?.RemoteAddress;

   private long _connectionsEstablished;
   private long _connectionsLost;

   public NetworkClientStats Stats => new()
   {
      ConnectionsEstablished = Interlocked.Read(ref _connectionsEstablished),
      ConnectionsLost = Interlocked.Read(ref _connectionsLost)
   };

   private readonly UdpTransportOptions _options = options;
   private UdpNetworkSession? _activeSession;

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      Socket? socket = null;
      try
      {
         TraceLogger.LogClientInfo("UDP ConnectAsync: Initiating UDP socket connection to {0}", endPoint);
         socket = new Socket(endPoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp);

         if (_options.SendBufferSize.HasValue)
         {
            socket.SendBufferSize = _options.SendBufferSize.Value;
         }
         if (_options.ReceiveBufferSize.HasValue)
         {
            socket.ReceiveBufferSize = _options.ReceiveBufferSize.Value;
         }

         await socket.ConnectAsync(endPoint, ct);
         TraceLogger.LogClientInfo("UDP ConnectAsync: UDP socket successfully connected to {0} (Local: {1})", socket.RemoteEndPoint, socket.LocalEndPoint);

         var localEndPoint = socket.LocalEndPoint ?? socket.RemoteEndPoint ?? endPoint;
         var remoteEndPoint = socket.RemoteEndPoint ?? endPoint;

         var session = new UdpNetworkSession(
            socket,
            localEndPoint,
            remoteEndPoint,
            _options,
            onDisposeAsync: _ => ValueTask.CompletedTask);

         var oldSession = Interlocked.Exchange(ref _activeSession, session);
         if (oldSession is not null)
         {
            await oldSession.DisposeAsync();
         }

         Interlocked.Increment(ref _connectionsEstablished);
         session.SessionClosedToken.Register(() => Interlocked.Increment(ref _connectionsLost));

         TraceLogger.LogClientInfo("UDP ConnectAsync: Network session {0} successfully established for {1}", session.Id, remoteEndPoint);
         return session;
      }
      catch (SocketException ex)
      {
         socket?.Dispose();
         TraceLogger.LogClientError("UDP ConnectAsync: Socket error connecting to {0}: {1}", endPoint, ex.Message);
         return new NetworkCodeError(ex.ErrorCode, ex.Message);
      }
      catch (Exception ex)
      {
         socket?.Dispose();
         TraceLogger.LogClientError("UDP ConnectAsync: Unexpected error connecting to {0}: {1}", endPoint, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public async ValueTask DisconnectAsync(CancellationToken ct = default)
   {
      var session = Interlocked.Exchange(ref _activeSession, null);
      if (session is not null)
      {
         await session.DisposeAsync();
      }
   }

   public async ValueTask DisposeAsync()
   {
      var session = Interlocked.Exchange(ref _activeSession, null);
      if (session is not null)
      {
         try
         {
            await session.DisposeAsync();
         }
         catch
         {
            // Ignored
         }
      }
   }
}

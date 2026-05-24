using System.Net;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpNetworkListener : INetworkListener
{
   public EndPoint LocalAddress { get; }

   private readonly TcpTransportOptions _options;

   public TcpNetworkListener(EndPoint localAddress, TcpTransportOptions options)
   {
      LocalAddress = localAddress;
      _options = options;

      
   }

   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }
}

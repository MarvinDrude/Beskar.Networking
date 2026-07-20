namespace Beskar.Networking.Abstractions.Interfaces;

public interface IClientFactory<out TSelf>
{
   public static abstract TSelf Create(INetworkClient client);
}

namespace Beskar.Networking.Abstractions.Enums;

public enum ConnectionState : byte
{
   Disconnected,
   Connecting,
   Connected,
   Reconnecting,
   Failed
}

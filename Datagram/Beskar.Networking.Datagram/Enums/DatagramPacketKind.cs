namespace Beskar.Networking.Datagram.Enums;

/// <summary>
/// Wire packet types encoded in the lower 4 bits of the datagram flags byte.
/// </summary>
internal enum DatagramPacketKind : byte
{
   ConnectRequest = 0,
   ConnectChallenge = 1,
   ConnectAccept = 2,
   ConnectAck = 3,
   Data = 4,
   AckOnly = 5,
   Disconnect = 6,
   TimeSyncPing = 7,
   TimeSyncPong = 8,
   PunchProbe = 9,
   RendezvousRegister = 10,
   RendezvousMapped = 11,
   RendezvousIntroduce = 12
}

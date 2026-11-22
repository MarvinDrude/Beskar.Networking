namespace Beskar.Datagram.ChaosSimulator;

public enum SimulatorProfile
{
   StandardChaos = 1,
   HighThroughputSoak = 2,
   ExtremePacketLossStorm = 3
}

public enum TransportBackend
{
   RealUdpSockets = 1,
   SimulatedChaosNetwork = 2
}

public enum ClientBehavior
{
   ActionPlayer,
   StateSyncFragmenter,
   RoamingMobile,
   FlakyCrasher,
   KeepAliveIdler,
   AuthRejectOrFuzz
}

namespace Beskar.Resilient.ChaosSimulator;

public enum ClientRole
{
   Sender,
   Echoer,
   KeepAliveOnly,
   Flaky,
   SlowReceiver,
   ChannelCongestor
}

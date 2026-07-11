using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Server.Extensions;

public static class ConnectReasonCodeExtensions
{
   extension(ConnectReasonCode code)
   {
      public ConnectReturnCode ToReturnCode => code switch
      {
         ConnectReasonCode.Success => ConnectReturnCode.Accepted,
         ConnectReasonCode.Banned or ConnectReasonCode.NotAuthorized => ConnectReturnCode.NotAuthorized,
         ConnectReasonCode.BadAuthenticationMethod or ConnectReasonCode.BadUserNameOrPassword => ConnectReturnCode.BadUserNameOrPassword,
         ConnectReasonCode.ClientIdentifierNotValid => ConnectReturnCode.IdentifierRejected,
         ConnectReasonCode.UseAnotherServer or ConnectReasonCode.ServerUnavailable
            or ConnectReasonCode.ServerBusy or ConnectReasonCode.ServerMoved => ConnectReturnCode.ServerUnavailable,
         _ => ConnectReturnCode.UnacceptableProtocolVersion
      };
   }
}

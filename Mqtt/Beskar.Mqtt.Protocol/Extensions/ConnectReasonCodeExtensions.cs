using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Extensions;

public static class ConnectReasonCodeExtensions
{
   extension(ConnectReasonCode value)
   {
      /// <summary>
      /// V5 -> V3
      /// </summary>
      public ConnectReturnCode ToV3ReturnCode()
      {
         return value switch
         {
            ConnectReasonCode.Success => ConnectReturnCode.Accepted,

            ConnectReasonCode.BadAuthenticationMethod or ConnectReasonCode.BadUserNameOrPassword
               => ConnectReturnCode.BadUserNameOrPassword,

            ConnectReasonCode.Banned or ConnectReasonCode.NotAuthorized
               => ConnectReturnCode.NotAuthorized,

            ConnectReasonCode.ClientIdentifierNotValid => ConnectReturnCode.IdentifierRejected,

            ConnectReasonCode.UseAnotherServer or ConnectReasonCode.ServerUnavailable
               or ConnectReasonCode.ServerBusy or ConnectReasonCode.ServerMoved
               => ConnectReturnCode.ServerUnavailable,

            _ => ConnectReturnCode.UnacceptableProtocolVersion
         };
      }
   }
}

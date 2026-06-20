namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// Authenticate Reason Code values.
/// </summary>
public enum AuthenticateReasonCode : byte
{
   /// <summary>
   /// Success (0x00).
   /// <para>Sent by: Server.</para>
   /// <para>Authentication is successful.</para>
   /// </summary>
   Success = 0x00,

   /// <summary>
   /// Continue authentication (0x18).
   /// <para>Sent by: Client or Server.</para>
   /// <para>Continue the authentication with another step.</para>
   /// </summary>
   ContinueAuthentication = 0x18,

   /// <summary>
   /// Re-authenticate (0x19).
   /// <para>Sent by: Client.</para>
   /// <para>Initiate a re-authentication.</para>
   /// </summary>
   ReAuthenticate = 0x19
}

using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram.Options;

/// <summary>
/// Cryptographic protection, handshake cookie, and endpoint mobility settings.
/// </summary>
public sealed class DatagramSecurityOptions
{
   /// <summary>
   /// Packet integrity and encryption mode. Defaults to <see cref="DatagramSecurityMode.Aead"/>.
   /// </summary>
   public DatagramSecurityMode Mode { get; set; } = DatagramSecurityMode.Aead;

   /// <summary>
   /// Cipher suite used when <see cref="Mode"/> is <see cref="DatagramSecurityMode.Aead"/>.
   /// </summary>
   public DatagramAeadCipher AeadCipher { get; set; } = DatagramAeadCipher.Auto;

   /// <summary>
   /// Optional pre-shared key mixed into HKDF handshake derivation to authenticate peers/servers.
   /// </summary>
   public byte[]? PreSharedKey { get; set; }

   /// <summary>
   /// Pads outbound <c>ConnectRequest</c> packets to <see cref="DatagramOptions.MaxDatagramSize"/>
   /// to prevent UDP reflection/amplification attacks. Defaults to <c>true</c>.
   /// </summary>
   public bool PadConnectRequests { get; set; } = true;

   /// <summary>
   /// Lifetime of stateless challenge cookies issued by the server. Defaults to 10 seconds.
   /// </summary>
   public TimeSpan ChallengeCookieLifetime { get; set; } = TimeSpan.FromSeconds(10);

   /// <summary>
   /// Allows an authenticated peer whose NAT/IP/port changes mid-session to migrate seamlessly
   /// when verified by valid AEAD/CRC + <c>SessionToken</c>. Defaults to <c>true</c>.
   /// </summary>
   public bool AllowEndpointMigration { get; set; } = true;
}

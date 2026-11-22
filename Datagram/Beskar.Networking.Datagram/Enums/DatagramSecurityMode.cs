namespace Beskar.Networking.Datagram.Enums;

/// <summary>
/// Specifies packet protection and encryption level for <see cref="DatagramHost"/>.
/// </summary>
public enum DatagramSecurityMode : byte
{
   /// <summary>
   /// No checksum or encryption beyond basic header validation. Fastest for local/loopback testing.
   /// </summary>
   None = 0,

   /// <summary>
   /// Appends a 4-byte CRC32C checksum for packet integrity without encryption.
   /// </summary>
   Crc32C = 1,

   /// <summary>
   /// Ephemeral ECDH P-256 + HKDF-SHA256 key exchange with per-packet AEAD encryption and authentication.
   /// </summary>
   Aead = 2
}

/// <summary>
/// Selects the AEAD cipher suite when <see cref="DatagramSecurityMode.Aead"/> is active.
/// </summary>
public enum DatagramAeadCipher : byte
{
   /// <summary>
   /// Prefer hardware-accelerated AES-GCM when available, otherwise ChaCha20-Poly1305.
   /// </summary>
   Auto = 0,

   /// <summary>
   /// ChaCha20-Poly1305 AEAD (16-byte tag).
   /// </summary>
   ChaCha20Poly1305 = 1,

   /// <summary>
   /// AES-256-GCM AEAD (16-byte tag).
   /// </summary>
   AesGcm = 2
}

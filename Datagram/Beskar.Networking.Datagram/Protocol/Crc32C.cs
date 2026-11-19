namespace Beskar.Networking.Datagram.Protocol;

/// <summary>
/// Castagnoli CRC-32C checksum implementation (polynomial 0x82F63B78 reflected).
/// </summary>
internal static class Crc32C
{
   public const int Size = 4;
   private const uint Polynomial = 0x82F63B78u;
   private static readonly uint[] Table = BuildTable();

   private static uint[] BuildTable()
   {
      var table = new uint[256];
      for (uint i = 0; i < 256; i++)
      {
         var crc = i;
         for (var bit = 0; bit < 8; bit++)
         {
            crc = (crc & 1) != 0 ? (crc >> 1) ^ Polynomial : crc >> 1;
         }
         table[i] = crc;
      }
      return table;
   }

   public static uint Compute(ReadOnlySpan<byte> data)
   {
      var crc = 0xFFFFFFFFu;
      var table = Table;

      foreach (var b in data)
      {
         crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8);
      }

      return ~crc;
   }
}

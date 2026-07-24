namespace Beskar.Networking.Resilient.Common.Enums;

/// <summary>
///
/// </summary>
public enum ResilientServerKeepAliveMode : byte
{
   /// <summary>
   /// No server support for keep-alive.
   /// </summary>
   None,

   /// <summary>
   /// Only checks keep-alive if client configured it on connect.
   /// </summary>
   ClientConfigured,

   /// <summary>
   /// The server always checks keep-alive, even if client did not configure it.
   /// (With the server default keep-alive)
   /// </summary>
   Alawys,
}

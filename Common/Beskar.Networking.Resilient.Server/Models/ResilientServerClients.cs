using System.Collections.Concurrent;
using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Server.Models;

/// <summary>
/// Manages active connected client sessions for a ResilientServer.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientServerClients<TFrame> : IAsyncDisposable
   where TFrame : struct, IFramingProtocol<TFrame>
{
   private readonly ConcurrentDictionary<Guid, ResilientServerClient<TFrame>> _clients = [];

   /// <summary>
   /// Gets the current number of connected clients.
   /// </summary>
   public int Count => _clients.Count;

   /// <summary>
   /// Attempts to register a newly connected client.
   /// </summary>
   public bool TryAdd(ResilientServerClient<TFrame> client)
   {
      return _clients.TryAdd(client.Id, client);
   }

   /// <summary>
   /// Attempts to remove a disconnected client.
   /// </summary>
   public bool TryRemove(Guid clientId, out ResilientServerClient<TFrame>? client)
   {
      return _clients.TryRemove(clientId, out client);
   }

   /// <summary>
   /// Retrieves a client by its unique ID.
   /// </summary>
   public bool TryGet(Guid clientId, out ResilientServerClient<TFrame>? client)
   {
      return _clients.TryGetValue(clientId, out client);
   }

   /// <summary>
   /// Returns a snapshot list of all connected clients.
   /// </summary>
   public IReadOnlyCollection<ResilientServerClient<TFrame>> GetAll()
   {
      return [.. _clients.Values];
   }

   /// <summary>
   /// Disconnects and clears all active client sessions.
   /// </summary>
   public async ValueTask DisconnectAllAsync()
   {
      var activeClients = _clients.Values.ToArray();
      _clients.Clear();

      foreach (var client in activeClients)
      {
         await client.DisposeAsync();
      }
   }

   public async ValueTask DisposeAsync()
   {
      await DisconnectAllAsync();
   }
}

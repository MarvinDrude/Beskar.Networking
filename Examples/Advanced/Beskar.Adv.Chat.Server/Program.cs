using System.Collections.Concurrent;
using System.Net;
using Beskar.Adv.Chat.Common;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Utilities.Tracing;
using Beskar.Adv.Chat.Server;

TraceLogger.IsEnabled = true;

Console.WriteLine("=================================================");
Console.WriteLine("Starting Chat Server...");
Console.WriteLine("=================================================");

var server = new ChatServerBuilder()
   .UseTcp(9000)
   .UseWs(11000)
   .UseQuic(12000)
   .Build();

var serverTask = Task.Run(async () =>
{
   try
   {
      await server.StartAsync();
   }
   catch (Exception ex)
   {
      Console.WriteLine($"[System] Server error: {ex.Message}");
   }
});

Console.WriteLine("[System] Chat Server is running. Press Enter to stop.");
Console.ReadLine();

Console.WriteLine("[System] Shutting down Server...");
await server.DisposeAsync();

try
{
   await serverTask;
}
catch (OperationCanceledException)
{
   // Expected
}

Console.WriteLine("[System] Chat Server stopped.");
return;

namespace Beskar.Adv.Chat.Server
{
   public sealed class ChatServer(INetworkListener[] listeners) : IAsyncDisposable
   {
      private readonly INetworkListener[] _listeners = listeners;
      private readonly ConcurrentDictionary<Guid, (string Username, MessageChannel Channel, INetworkSession Session)> _clients = new();
      private readonly List<ChatMessage> _history = [];

      private readonly Lock _historyLock = new();
      private readonly CancellationTokenSource _cts = new();

      private bool _disposed;

      public async Task StartAsync()
      {
         var token = _cts.Token;
         var bindTasks = _listeners.Select(async listener =>
         {
            var bindResult = await listener.BindAsync(token);
            if (bindResult.Failed)
            {
               throw new InvalidOperationException($"Failed to bind listener on {listener.LocalAddress}: {bindResult.Error.Message}");
            }

            _ = Task.Run(() => AcceptLoopAsync(listener, token), token);
         });

         await Task.WhenAll(bindTasks);
         Console.WriteLine("[Server] All listeners bound and listening successfully.");
      }

      private async Task AcceptLoopAsync(INetworkListener listener, CancellationToken token)
      {
         while (!token.IsCancellationRequested)
         {
            try
            {
               var sessionResult = await listener.AcceptSessionAsync(token);
               if (sessionResult.Failed)
               {
                  continue;
               }

               _ = Task.Factory.StartNew(
                  () => RunClientTask(sessionResult.Success, token),
                  TaskCreationOptions.PreferFairness);
            }
            catch (Exception)
            {
               // Ignored
            }
         }
      }

      private async Task RunClientTask(INetworkSession session, CancellationToken ct)
      {
         var streamResult = await session.AcceptStreamAsync(ct);
         if (streamResult.Failed)
         {
            await session.DisposeAsync();
            return;
         }

         var channel = new MessageChannel(streamResult.Success.Transport);

         try
         {
            // 1. Handshake - Expect Join Packet
            var joinPacket = await channel.ReadPacketAsync(ct);
            if (joinPacket?.Type is not PacketType.Join)
            {
               return;
            }

            var joinPayload = joinPacket.AsJson<JoinPayload>();
            if (joinPayload is null || string.IsNullOrWhiteSpace(joinPayload.Username))
            {
               return;
            }

            var username = joinPayload.Username;
            Console.WriteLine($"[Server] Client joined: {username} ({session.RemoteAddress})");

            // 2. Retrieve history and send Welcome Packet
            List<ChatMessage> historySnapshot;
            lock (_historyLock)
            {
               historySnapshot = [.. _history];
            }

            var welcomePayload = new WelcomePayload
            {
               Username = username,
               History = historySnapshot
            };

            await channel.WritePacketAsync(ChatPacket.CreateJson(PacketType.Welcome, welcomePayload), ct);

            // 3. Add to client list
            _clients[session.Id] = (username, channel, session);

            // 4. Broadcast join message
            var joinMsg = new ChatMessage
            {
               Sender = "System",
               Text = $"{username} joined the chat.",
               Timestamp = DateTime.Now
            };
            await BroadcastMessageAsync(joinMsg, ct);

            // 5. Read messages loop
            while (!ct.IsCancellationRequested && !session.SessionClosedToken.IsCancellationRequested)
            {
               var packet = await channel.ReadPacketAsync(ct);
               if (packet is null)
               {
                  break; // Disconnected
               }

               if (packet.Type != PacketType.Message)
               {
                  continue;
               }

               var chatMessage = packet.AsJson<ChatMessage>();
               if (chatMessage is null)
               {
                  continue;
               }

               // Enrich message
               chatMessage.Sender = username;
               chatMessage.Timestamp = DateTime.Now;

               // Add to history
               lock (_historyLock)
               {
                  _history.Add(chatMessage);
                  if (_history.Count > 50)
                  {
                     _history.RemoveAt(0);
                  }
               }

               // Broadcast message
               await BroadcastMessageAsync(chatMessage, ct);
            }
         }
         catch (Exception ex)
         {
            Console.WriteLine($"[Server] Error processing client {session.Id}: {ex.Message}");
         }
         finally
         {
            if (_clients.TryRemove(session.Id, out var clientInfo))
            {
               Console.WriteLine($"[Server] Client left: {clientInfo.Username}");

               var leaveMsg = new ChatMessage
               {
                  Sender = "System",
                  Text = $"{clientInfo.Username} left the chat.",
                  Timestamp = DateTime.Now
               };

               _ = Task.Run(async () =>
               {
                  try
                  {
                     await BroadcastMessageAsync(leaveMsg, CancellationToken.None);
                  }
                  catch
                  {
                     // Ignored
                  }
               }, ct);
            }

            await session.DisposeAsync();
         }
      }

      private async Task BroadcastMessageAsync(ChatMessage message, CancellationToken ct)
      {
         var packet = ChatPacket.CreateJson(PacketType.Message, message);
         var tasks = _clients.Values.Select(async client =>
         {
            try
            {
               await client.Channel.WritePacketAsync(packet, ct);
            }
            catch
            {
               // Ignored
            }
         });

         await Task.WhenAll(tasks);
      }

      public async ValueTask DisposeAsync()
      {
         if (_disposed) return;
         _disposed = true;

         await _cts.CancelAsync();
         _cts.Dispose();

         var unbindTasks = _listeners.Select(async listener =>
         {
            try
            {
               await listener.UnbindAsync();
            }
            catch
            {
               // Ignored
            }
         });
         await Task.WhenAll(unbindTasks);

         var clientDisposals = _clients.Values.Select(async client =>
         {
            try
            {
               await client.Session.DisposeAsync();
            }
            catch
            {
               // Ignored
            }
         });
         await Task.WhenAll(clientDisposals);

         foreach (var listener in _listeners)
         {
            try
            {
               await listener.DisposeAsync();
            }
            catch
            {
               // Ignored
            }
         }
      }
   }
}

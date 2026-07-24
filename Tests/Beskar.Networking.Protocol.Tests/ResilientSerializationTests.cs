using System.Buffers;
using System.Net;
using System.Text.Json;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;

namespace Beskar.Networking.Protocol.Tests;

public class ResilientSerializationTests
{
   private sealed class JsonResilientSerializer : IResilientSerializer
   {
      public void Serialize<T>(T value, IBufferWriter<byte> writer)
      {
         using var jsonWriter = new Utf8JsonWriter(writer);
         JsonSerializer.Serialize(jsonWriter, value);
      }

      public T? Deserialize<T>(in ReadOnlySequence<byte> sequence)
      {
         var reader = new Utf8JsonReader(sequence);
         return JsonSerializer.Deserialize<T>(ref reader);
      }
   }

   public record TestUserPayload(int Id, string Username, string Email, DateTime CreatedAt);

   [Test]
   public async Task SendPayloadAsync_WithCustomJsonSerializer_ShouldRoundtripObject()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var serializer = new JsonResilientSerializer();

      var serverOptions = new ResilientServerOptions
      {
         Serializer = serializer
      };

      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>(serverOptions)
         .UseTcp(listenerEndPoint)
         .Build();

      var serverReceivedTcs = new TaskCompletionSource<TestUserPayload>();

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var user = ctx.Client.DeserializePayload<TestUserPayload>(ctx.Frame);
         if (user != null)
         {
            serverReceivedTcs.TrySetResult(user);
         }
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var clientOptions = new ResilientClientOptions
      {
         Serializer = serializer,
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      };

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);
      var connectResult = await client.ConnectAsync(boundEndPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var originalUser = new TestUserPayload(42, "AntigravityUser", "antigravity@beskar.net", DateTime.UtcNow);
      await client.SendPayloadAsync(originalUser);

      var receivedUser = await serverReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(receivedUser.Id).IsEqualTo(42);
      await Assert.That(receivedUser.Username).IsEqualTo("AntigravityUser");
      await Assert.That(receivedUser.Email).IsEqualTo("antigravity@beskar.net");

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task SendPayloadAsync_LargePayload_1MB_ShouldRoundtrip()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var serializer = new JsonResilientSerializer();

      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>(new ResilientServerOptions { Serializer = serializer })
         .UseTcp(listenerEndPoint)
         .Build();

      var serverReceivedTcs = new TaskCompletionSource<byte[]>();

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var data = ctx.Client.DeserializePayload<byte[]>(ctx.Frame);
         if (data != null)
         {
            serverReceivedTcs.TrySetResult(data);
         }
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;
      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
      {
         Serializer = serializer,
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      });

      await client.ConnectAsync(boundEndPoint);

      var largeArray = new byte[1024 * 1024]; // 1MB
      Random.Shared.NextBytes(largeArray);

      await client.SendPayloadAsync(largeArray);

      var receivedArray = await serverReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(receivedArray.Length).IsEqualTo(largeArray.Length);
      await Assert.That(receivedArray).IsEquivalentTo(largeArray);

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}

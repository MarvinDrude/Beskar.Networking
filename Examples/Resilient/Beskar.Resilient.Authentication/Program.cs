using System.Net;
using System.Security.Cryptography;
using System.Text;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;

Console.WriteLine("=================================================");
Console.WriteLine("Resilient Challenge-Response Authentication Example");
Console.WriteLine("=================================================");

var endPoint = new IPEndPoint(IPAddress.Loopback, 9002);
var serverSharedKey = "my-super-secret-key-12345"u8.ToArray();

// 1. Configure and Build the Resilient Server
var server = ResilientServerFactory.CreateBuilder()
   .UseTcp(endPoint)
   .Build();

server.Events.OnConnect.Add(async (ctx, ct) =>
{
   Console.WriteLine(
      $"[Server] Client connecting from {ctx.Client.Session.RemoteAddress}. Initiating auth challenge...");

   // Generate a 32-byte cryptographically secure random challenge
   var challengeBytes = new byte[32];
   RandomNumberGenerator.Fill(challengeBytes);

   var challenge = new AuthenticatePacketPayload
   {
      AuthMethod = "HMAC-SHA256",
      AuthData = challengeBytes
   };

   // Send the challenge to the client
   await ctx.SendAuthenticateAsync(challenge, ct);

   // Await the client's signature response
   var response = await ctx.ReceiveAuthenticateAsync(ct);

   if (response is null || response.AuthMethod != "HMAC-SHA256" || response.AuthData is null)
   {
      Console.WriteLine("[Server] Auth failed: Invalid auth method or empty response.");
      ctx.Deny();
      return;
   }

   // Recompute signature to verify client knows the shared key
   byte[] expectedSignature;
   using (var hmac = new HMACSHA256(serverSharedKey))
   {
      expectedSignature = hmac.ComputeHash(challengeBytes);
   }

   if (!CryptographicOperations.FixedTimeEquals(response.AuthData, expectedSignature))
   {
      Console.WriteLine("[Server] Auth failed: Signature verification failed. Denying client.");
      ctx.Deny();
      return;
   }

   Console.WriteLine($"[Server] Client {ctx.Client.Id} authenticated successfully.");
});

await server.StartAsync();

// 2. Test Case 1: Connect with the CORRECT shared secret key
Console.WriteLine("\n--- Test Case 1: Connecting with CORRECT key ---");
var correctClientKey = "my-super-secret-key-12345"u8.ToArray();

var client1 = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
{
   Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
});

client1.Events.OnAuthenticate.Add(async (ctx, ct) =>
{
   Console.WriteLine("[Client 1] Challenge received from server. Computing signature...");
   var challenge = ctx.ChallengePayload;

   if (challenge.AuthMethod == "HMAC-SHA256" && challenge.AuthData != null)
   {
      byte[] signature;
      using (var hmac = new HMACSHA256(correctClientKey))
      {
         signature = hmac.ComputeHash(challenge.AuthData);
      }

      var response = new AuthenticatePacketPayload
      {
         AuthMethod = "HMAC-SHA256",
         AuthData = signature
      };

      await ctx.SendAuthenticateResponseAsync(response, ct);
   }
});

var result1 = await client1.ConnectAsync(endPoint);
if (result1.Failed)
{
   Console.WriteLine($"[Client 1] Connection failed: {result1.Error.Detail}");
}
else
{
   Console.WriteLine("[Client 1] Connection succeeded! Authenticated client is connected.");
   await client1.DisconnectAsync();
}

await client1.DisposeAsync();

// 3. Test Case 2: Connect with an INCORRECT shared secret key
Console.WriteLine("\n--- Test Case 2: Connecting with INCORRECT key ---");
var incorrectClientKey = "wrong-key-code-abc"u8.ToArray();

var client2 = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
{
   Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
});

client2.Events.OnAuthenticate.Add(async (ctx, ct) =>
{
   Console.WriteLine("[Client 2] Challenge received. Computing signature with WRONG key...");
   var challenge = ctx.ChallengePayload;

   if (challenge.AuthMethod == "HMAC-SHA256" && challenge.AuthData != null)
   {
      byte[] signature;
      using (var hmac = new HMACSHA256(incorrectClientKey))
      {
         signature = hmac.ComputeHash(challenge.AuthData);
      }

      var response = new AuthenticatePacketPayload
      {
         AuthMethod = "HMAC-SHA256",
         AuthData = signature
      };

      await ctx.SendAuthenticateResponseAsync(response, ct);
   }
});

var result2 = await client2.ConnectAsync(endPoint);
if (result2.Failed)
{
   Console.WriteLine($"[Client 2] Connection failed as expected: {result2.Error.Detail}");
}
else
{
   Console.WriteLine("[Client 2] ERROR: Connection succeeded with WRONG key.");
   await client2.DisconnectAsync();
}

await client2.DisposeAsync();

// 4. Shutdown
Console.WriteLine("\n[System] Stopping server...");
await server.DisposeAsync();
Console.WriteLine("[System] Done.");

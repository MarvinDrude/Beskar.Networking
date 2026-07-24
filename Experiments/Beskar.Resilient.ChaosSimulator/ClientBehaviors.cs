using System.Buffers;
using System.Text;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Resilient.Client;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;

namespace Beskar.Resilient.ChaosSimulator;

public static class ClientBehaviors
{
   public static async Task ExecuteSenderBehaviorAsync(
      ResilientClient<BeskarPacket> client, string clientId, string transport, CancellationToken ct)
   {
      var sendCount = Random.Shared.Next(5, 12);

      for (var i = 0; i < sendCount && !ct.IsCancellationRequested; i++)
      {
         var payloadStr = $"sender-payload-{Random.Shared.Next(100)}";
         var payloadBytes = Encoding.UTF8.GetBytes(payloadStr);
         var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payloadBytes));

         Program.LogChaos("CLIENT", transport, "SEND",
            $"Client '{clientId}' sending frame ({payloadBytes.Length} bytes).", ConsoleColor.Yellow);

         var result = await SendWithTimeoutAsync(client, frame, ct);

         if (!result.Failed)
         {
            Interlocked.Increment(ref Program.ClientMessagesSent);
         }
         else
         {
            Program.LogChaos("CLIENT", transport, "SEND_ERR",
               $"Client '{clientId}' send failed: {result.Error.Detail}", ConsoleColor.DarkRed, true);
         }

         await Task.Delay(Random.Shared.Next(800, 2000), ct);
      }
   }

   public static async Task ExecuteEchoerBehaviorAsync(
      ResilientClient<BeskarPacket> client, string clientId, string transport, CancellationToken ct)
   {
      // Add receive handler
      using var receiveToken = client.Events.FrameReceived.Add((context, token) =>
      {
         if (context.Frame.GetFrameKind() == ResilientFrameKind.Message)
         {
            Interlocked.Increment(ref Program.ClientMessagesReceived);
            var payload = Encoding.UTF8.GetString(context.Frame.Payload.ToArray());
            Program.LogChaos("CLIENT", transport, "RECEIVE",
               $"Client '{clientId}' received echoed message: '{payload}'", ConsoleColor.Green);
         }
         return ValueTask.CompletedTask;
      });

      var sendCount = Random.Shared.Next(5, 10);
      for (var i = 0; i < sendCount && !ct.IsCancellationRequested; i++)
      {
         var payloadStr = $"echo-val-{Random.Shared.Next(1000)}";
         var payloadBytes = Encoding.UTF8.GetBytes(payloadStr);
         var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payloadBytes));

         Program.LogChaos("CLIENT", transport, "ECHO_SEND",
            $"Client '{clientId}' sending message for echo: '{payloadStr}'", ConsoleColor.Cyan);

         var result = await SendWithTimeoutAsync(client, frame, ct);

         if (!result.Failed)
         {
            Interlocked.Increment(ref Program.ClientMessagesSent);
         }
         else
         {
            Program.LogChaos("CLIENT", transport, "SEND_ERR",
               $"Client '{clientId}' echo send failed: {result.Error.Detail}", ConsoleColor.Red, true);
         }

         await Task.Delay(Random.Shared.Next(1000, 2500), ct);
      }
   }

   public static async Task ExecuteKeepAliveBehaviorAsync(
      ResilientClient<BeskarPacket> client, string clientId, string transport, CancellationToken ct)
   {
      Program.LogChaos("CLIENT", transport, "IDLE",
         $"Client '{clientId}' registered with low keep-alive. Monitoring ping/pong...", ConsoleColor.DarkCyan);

      var duration = TimeSpan.FromSeconds(Random.Shared.Next(15, 25));
      var end = DateTimeOffset.UtcNow + duration;

      while (DateTimeOffset.UtcNow < end && !ct.IsCancellationRequested)
      {
         await Task.Delay(1000, ct);
         Interlocked.Increment(ref Program.ClientPingsSent);
      }
   }

   public static async Task ExecuteFlakyBehaviorAsync(
      ResilientClient<BeskarPacket> client, string clientId, string transport, CancellationToken ct)
   {
      var payloadStr = "FLAKY-BURST-DATA";
      var payloadBytes = Encoding.UTF8.GetBytes(payloadStr);
      var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payloadBytes));

      Program.LogChaos("CLIENT", transport, "FLAKY_SEND",
         $"Client '{clientId}' sending quick flaky payload...", ConsoleColor.Yellow);
      
      var result = await SendWithTimeoutAsync(client, frame, ct);
      if (!result.Failed)
      {
         Interlocked.Increment(ref Program.ClientMessagesSent);
      }
      else
      {
         Program.LogChaos("CLIENT", transport, "SEND_ERR",
            $"Client '{clientId}' flaky send failed: {result.Error.Detail}", ConsoleColor.DarkRed, true);
      }

      await Task.Delay(Random.Shared.Next(100, 500), ct);
   }

   public static async Task ExecuteSlowReceiverBehaviorAsync(
      ResilientClient<BeskarPacket> client, string clientId, string transport, CancellationToken ct)
   {
      using var receiveToken = client.Events.FrameReceived.Add(async (context, token) =>
      {
         if (context.Frame.GetFrameKind() == ResilientFrameKind.Message)
         {
            Interlocked.Increment(ref Program.ClientMessagesReceived);
            var payload = Encoding.UTF8.GetString(context.Frame.Payload.ToArray());
            Program.LogChaos("CLIENT", transport, "SLOW_RCV_START",
               $"Client '{clientId}' starting slow processing of message: '{payload}'", ConsoleColor.DarkGreen);
            try
            {
               await Task.Delay(1500, token); // simulate slow message processing
            }
            catch (OperationCanceledException)
            {
            }

            Program.LogChaos("CLIENT", transport, "SLOW_RCV_END",
               $"Client '{clientId}' finished slow processing of message.", ConsoleColor.Green);
         }
      });

      // Keep connection open to receive messages
      await Task.Delay(TimeSpan.FromSeconds(20), ct);
   }

   public static async Task ExecuteChannelCongestorBehaviorAsync(
      ResilientClient<BeskarPacket> client, string clientId, string transport, CancellationToken ct)
   {
      Program.LogChaos("CLIENT", transport, "CONGEST",
         $"Client '{clientId}' starting high-speed message congestor...", ConsoleColor.Yellow);
      
      var end = DateTimeOffset.UtcNow.AddSeconds(10);
      var payloadStr = "CONGESTION-FIREHOSE-DATA-PACKET-12345";
      var payloadBytes = Encoding.UTF8.GetBytes(payloadStr);
      var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payloadBytes));

      while (DateTimeOffset.UtcNow < end && !ct.IsCancellationRequested)
      {
         var result = await SendWithTimeoutAsync(client, frame, ct);
         if (!result.Failed)
         {
            Interlocked.Increment(ref Program.ClientMessagesSent);
         }
         else
         {
            Program.LogChaos("CLIENT", transport, "SEND_ERR",
               $"Client '{clientId}' congestor send failed: {result.Error.Detail}", ConsoleColor.DarkRed, true);
         }

         await Task.Delay(50, ct);
      }
   }

   private static async Task<Result<VoidResult<StringError>, StringError>> SendWithTimeoutAsync(
      ResilientClient<BeskarPacket> client, BeskarPacket frame, CancellationToken ct)
   {
      using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      cts.CancelAfter(TimeSpan.FromSeconds(5));
      try
      {
         await client.SendAsync(frame, cts.Token);
         return new VoidResult<StringError>();
      }
      catch (Exception ex)
      {
         return new StringError($"Send failed or timed out: {ex.Message}");
      }
   }
}

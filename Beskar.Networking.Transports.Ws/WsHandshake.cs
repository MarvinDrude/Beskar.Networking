using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// Handles HTTP/1.1 handshake negotiations for establishing WebSocket connections.
/// </summary>
public static class WsHandshake
{
   private const string MagicGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

   /// <summary>
   /// Computes the Sec-WebSocket-Accept key response for a given client key.
   /// </summary>
   public static string ComputeAcceptKey(string secWebSocketKey)
   {
      var combined = secWebSocketKey + MagicGuid;
      var bytes = Encoding.ASCII.GetBytes(combined);
      var hash = SHA1.HashData(bytes);
      return Convert.ToBase64String(hash);
   }

   /// <summary>
   /// Performs the server-side HTTP/1.1 WebSocket upgrade handshake.
   /// </summary>
   public static async Task<string?> ServerHandshakeAsync(
      IDuplexPipe tcpPipe,
      WsTransportOptions options,
      CancellationToken ct)
   {
      var reader = tcpPipe.Input;
      var writer = tcpPipe.Output;

      var headersText = await ReadHttpHeadersAsync(reader, ct);
      if (headersText == null)
      {
         return null;
      }

      var lines = headersText.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
      if (lines.Length == 0 || !lines[0].StartsWith("GET ", StringComparison.OrdinalIgnoreCase))
      {
         await SendErrorResponseAsync(writer, "400 Bad Request", "Only GET requests are allowed.");
         return null;
      }

      var path = lines[0].Split(' ')[1];
      if (path != options.Path)
      {
         await SendErrorResponseAsync(writer, "404 Not Found", "Specified path is not found.");
         return null;
      }

      string? clientKey = null;
      var isUpgrade = false;
      var isConnectionUpgrade = false;

      for (var i = 1; i < lines.Length; i++)
      {
         var line = lines[i];
         var colonIdx = line.IndexOf(':');
         if (colonIdx == -1) continue;

         var headerName = line[..colonIdx].Trim().ToLowerInvariant();
         var headerValue = line[(colonIdx + 1)..].Trim();

         if (headerName == "upgrade" && headerValue.Equals("websocket", StringComparison.OrdinalIgnoreCase))
         {
            isUpgrade = true;
         }
         else if (headerName == "connection" && headerValue.Contains("upgrade", StringComparison.OrdinalIgnoreCase))
         {
            isConnectionUpgrade = true;
         }
         else if (headerName == "sec-websocket-key")
         {
            clientKey = headerValue;
         }
      }

      if (!isUpgrade || !isConnectionUpgrade || string.IsNullOrEmpty(clientKey))
      {
         await SendErrorResponseAsync(writer, "400 Bad Request", "Invalid WebSocket upgrade headers.");
         return null;
      }

      // Complete handshake
      var acceptKey = ComputeAcceptKey(clientKey);
      var response = new StringBuilder();
      response.Append("HTTP/1.1 101 Switching Protocols\r\n");
      response.Append("Upgrade: websocket\r\n");
      response.Append("Connection: Upgrade\r\n");
      response.Append($"Sec-WebSocket-Accept: {acceptKey}\r\n");

      if (!string.IsNullOrEmpty(options.Subprotocol))
      {
         response.Append($"Sec-WebSocket-Protocol: {options.Subprotocol}\r\n");
      }

      response.Append("\r\n");

      var responseBytes = Encoding.ASCII.GetBytes(response.ToString());
      writer.Write(responseBytes);

      await writer.FlushAsync(ct);

      return acceptKey;
   }

   /// <summary>
   /// Performs the client-side HTTP/1.1 WebSocket upgrade handshake.
   /// </summary>
   public static async Task<bool> ClientHandshakeAsync(
      IDuplexPipe tcpPipe,
      EndPoint endPoint,
      WsTransportOptions options,
      CancellationToken ct)
   {
      var reader = tcpPipe.Input;
      var writer = tcpPipe.Output;

      // Generate WebSocket Key
      var randomBytes = new byte[16];
      RandomNumberGenerator.Fill(randomBytes);
      var secWebSocketKey = Convert.ToBase64String(randomBytes);
      var expectedAcceptKey = ComputeAcceptKey(secWebSocketKey);

      var host = endPoint.ToString() ?? "localhost";

      var request = new StringBuilder();
      request.Append($"GET {options.Path} HTTP/1.1\r\n");
      request.Append($"Host: {host}\r\n");
      request.Append("Upgrade: websocket\r\n");
      request.Append("Connection: Upgrade\r\n");
      request.Append($"Sec-WebSocket-Key: {secWebSocketKey}\r\n");
      request.Append("Sec-WebSocket-Version: 13\r\n");

      if (!string.IsNullOrEmpty(options.Subprotocol))
      {
         request.Append($"Sec-WebSocket-Protocol: {options.Subprotocol}\r\n");
      }
      request.Append("\r\n");

      var requestBytes = Encoding.ASCII.GetBytes(request.ToString());
      writer.Write(requestBytes);

      await writer.FlushAsync(ct);

      // Read response headers
      var responseHeaders = await ReadHttpHeadersAsync(reader, ct);
      if (responseHeaders == null)
      {
         return false;
      }

      var lines = responseHeaders.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
      if (lines.Length == 0 || !lines[0].Contains("101"))
      {
         return false;
      }

      var serverAcceptKeyMatched = false;
      for (var i = 1; i < lines.Length; i++)
      {
         var line = lines[i];
         var colonIdx = line.IndexOf(':');
         if (colonIdx == -1) continue;

         var headerName = line[..colonIdx].Trim().ToLowerInvariant();
         var headerValue = line[(colonIdx + 1)..].Trim();

         if (headerName == "sec-websocket-accept" && headerValue == expectedAcceptKey)
         {
            serverAcceptKeyMatched = true;
         }
      }

      return serverAcceptKeyMatched;
   }

   private static async Task<string?> ReadHttpHeadersAsync(PipeReader reader, CancellationToken ct)
   {
      while (true)
      {
         var result = await reader.ReadAsync(ct);
         var buffer = result.Buffer;

         // Check for \r\n\r\n delimiter marking end of headers
         var position = FindSequence(buffer, "\r\n\r\n"u8.ToArray());
         if (position.HasValue)
         {
            var headerSequence = buffer.Slice(0, position.Value);
            var headerText = Encoding.ASCII.GetString(headerSequence.ToArray());

            // Advance reader past the \r\n\r\n
            reader.AdvanceTo(buffer.GetPosition(4, position.Value));
            return headerText;
         }

         reader.AdvanceTo(buffer.Start, buffer.End);

         if (result.IsCompleted || result.IsCanceled)
         {
            return null;
         }
      }
   }

   private static SequencePosition? FindSequence(ReadOnlySequence<byte> buffer, byte[] sequence)
   {
      if (buffer.Length < sequence.Length) return null;

      var position = buffer.Start;
      while (buffer.TryGet(ref position, out var memory))
      {
         var index = memory.Span.IndexOf(sequence[0]);

         if (index != -1)
         {
            var candidate = buffer.GetPosition(index, position);
            if (Matches(buffer.Slice(candidate), sequence))
            {
               return candidate;
            }
         }

         if (position.GetObject() == null) break;
      }

      return null;
   }

   private static bool Matches(ReadOnlySequence<byte> slice, byte[] sequence)
   {
      if (slice.Length < sequence.Length) return false;

      var index = 0;
      var position = slice.Start;

      while (slice.TryGet(ref position, out var memory) && index < sequence.Length)
      {
         var span = memory.Span;
         for (var i = 0; i < span.Length && index < sequence.Length; i++)
         {
            if (span[i] != sequence[index++])
            {
               return false;
            }
         }
      }

      return true;
   }

   private static async Task SendErrorResponseAsync(PipeWriter writer, string status, string message)
   {
      var response = $"HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{message}";
      writer.Write(Encoding.ASCII.GetBytes(response));

      await writer.FlushAsync();
   }
}

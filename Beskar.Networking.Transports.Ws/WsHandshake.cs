using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Buffers;
using Beskar.Memory.Owners;
using Beskar.Memory.Pools;
using Beskar.Memory.Writers;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// Handles HTTP/1.1 handshake negotiations for establishing WebSocket connections.
/// </summary>
public static class WsHandshake
{
   private const string MagicGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
   private static readonly byte[] EndOfHeadersSequence = "\r\n\r\n"u8.ToArray();

   private const string HttpVersionPrefix = "HTTP/1.1 ";
   private const string ErrorResponseHeaders = "\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n";

   /// <summary>
   /// Computes the Sec-WebSocket-Accept key response for a given client key.
   /// </summary>
   public static string ComputeAcceptKey(string secWebSocketKey)
   {
      ArgumentException.ThrowIfNullOrEmpty(secWebSocketKey);
      if (secWebSocketKey.Length > 128)
      {
         throw new ArgumentException("Key cannot be longer than 128 characters.", nameof(secWebSocketKey));
      }

      Span<char> combined = stackalloc char[secWebSocketKey.Length + 36];
      secWebSocketKey.AsSpan().CopyTo(combined);
      MagicGuid.AsSpan().CopyTo(combined[secWebSocketKey.Length..]);

      Span<byte> bytes = stackalloc byte[combined.Length];
      Encoding.ASCII.GetBytes(combined, bytes);

      Span<byte> hash = stackalloc byte[20];
      SHA1.HashData(bytes, hash);

      return Convert.ToBase64String(hash);
   }

   /// <summary>
   /// Performs the server-side HTTP/1.1 WebSocket upgrade handshake.
   /// </summary>
   public static async Task<(string? AcceptKey, Dictionary<string, string>? Headers, Dictionary<string, string>? Cookies)> ServerHandshakeAsync(
      IDuplexPipe tcpPipe,
      WsTransportOptions options,
      CancellationToken ct)
   {
      TraceLogger.LogServerInfo("WS Handshake: Starting server WebSocket upgrade handshake on path {0}", options.Path);
      var reader = tcpPipe.Input;
      var writer = tcpPipe.Output;

      using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      timeoutCts.CancelAfter(options.HandshakeTimeout);

      string? headersText;
      try
      {
         headersText = await ReadHttpHeadersAsync(reader, options.MaxHeaderSize, timeoutCts.Token);
      }
      catch (OperationCanceledException)
      {
         TraceLogger.LogServerError("WS Handshake: Failed to read HTTP headers. Handshake timed out.");
         return (null, null, null);
      }

      if (headersText == null)
      {
         TraceLogger.LogServerError("WS Handshake: Failed to read HTTP headers from client or headers exceeded limits.");
         return (null, null, null);
      }

      var remaining = headersText.AsSpan();

      // Parse the first line (GET /path HTTP/1.1)
      var firstLineEnd = remaining.IndexOf("\r\n".AsSpan());
      ReadOnlySpan<char> firstLine;
      if (firstLineEnd == -1)
      {
         firstLine = remaining;
         remaining = default;
      }
      else
      {
         firstLine = remaining[..firstLineEnd];
         remaining = remaining[(firstLineEnd + 2)..];
      }

      if (!firstLine.StartsWith("GET ".AsSpan(), StringComparison.OrdinalIgnoreCase))
      {
         TraceLogger.LogServerError("WS Handshake: Server handshake failed: only GET requests are allowed.");
         await SendErrorResponseAsync(writer, "400 Bad Request", "Only GET requests are allowed.");
         return (null, null, null);
      }

      var firstSpace = firstLine.IndexOf(' ');
      if (firstSpace == -1)
      {
         TraceLogger.LogServerError("WS Handshake: Server handshake failed: invalid GET request format.");
         return (null, null, null);
      }

      var afterGet = firstLine[(firstSpace + 1)..];
      var secondSpace = afterGet.IndexOf(' ');
      if (secondSpace == -1)
      {
         TraceLogger.LogServerError("WS Handshake: Server handshake failed: invalid GET request format.");
         return (null, null, null);
      }

      var pathSpan = afterGet[..secondSpace];
      if (!pathSpan.Equals(options.Path.AsSpan(), StringComparison.Ordinal))
      {
         TraceLogger.LogServerError("WS Handshake: Server handshake failed: specified path does not match expected path.");
         await SendErrorResponseAsync(writer, "404 Not Found", "Specified path is not found.");
         return (null, null, null);
      }

      string? clientKey = null;
      var isUpgrade = false;
      var isConnectionUpgrade = false;
      string? origin = null;

      Dictionary<string, string>? requestHeaders = null;
      Dictionary<string, string>? requestCookies = null;

      while (!remaining.IsEmpty)
      {
         var lineEnd = remaining.IndexOf("\r\n".AsSpan());
         ReadOnlySpan<char> line;
         if (lineEnd == -1)
         {
            line = remaining;
            remaining = default;
         }
         else
         {
            line = remaining[..lineEnd];
            remaining = remaining[(lineEnd + 2)..];
         }

         if (line.IsEmpty) continue;

         var colonIdx = line.IndexOf(':');
         if (colonIdx == -1) continue;

         var headerNameSpan = line[..colonIdx].Trim();
         var headerValueSpan = line[(colonIdx + 1)..].Trim();

         if (options.GatherHeaders)
         {
            requestHeaders ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var lookup = requestHeaders.GetAlternateLookup<ReadOnlySpan<char>>();
            lookup[headerNameSpan] = headerValueSpan.ToString();
         }

         if (headerNameSpan.Equals("upgrade".AsSpan(), StringComparison.OrdinalIgnoreCase))
         {
            if (headerValueSpan.Equals("websocket".AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
               isUpgrade = true;
            }
         }
         else if (headerNameSpan.Equals("connection".AsSpan(), StringComparison.OrdinalIgnoreCase))
         {
            if (headerValueSpan.Contains("upgrade".AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
               isConnectionUpgrade = true;
            }
         }
         else if (headerNameSpan.Equals("sec-websocket-key".AsSpan(), StringComparison.OrdinalIgnoreCase))
         {
            clientKey = headerValueSpan.ToString();
         }
         else if (headerNameSpan.Equals("origin".AsSpan(), StringComparison.OrdinalIgnoreCase))
         {
            origin = headerValueSpan.ToString();
         }
         else if (headerNameSpan.Equals("cookie".AsSpan(), StringComparison.OrdinalIgnoreCase) && options.GatherCookies)
         {
            requestCookies ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var lookup = requestCookies.GetAlternateLookup<ReadOnlySpan<char>>();

            var cookieRemaining = headerValueSpan;
            while (!cookieRemaining.IsEmpty)
            {
               var semiIdx = cookieRemaining.IndexOf(';');
               ReadOnlySpan<char> cookiePair;

               if (semiIdx == -1)
               {
                  cookiePair = cookieRemaining;
                  cookieRemaining = default;
               }
               else
               {
                  cookiePair = cookieRemaining[..semiIdx];
                  cookieRemaining = cookieRemaining[(semiIdx + 1)..];
               }

               cookiePair = cookiePair.Trim();
               if (cookiePair.IsEmpty) continue;

               var eqIdx = cookiePair.IndexOf('=');
               if (eqIdx != -1)
               {
                  var nameSpan = cookiePair[..eqIdx].Trim();
                  var valueSpan = cookiePair[(eqIdx + 1)..].Trim();
                  lookup[nameSpan] = valueSpan.ToString();
               }
            }
         }
      }

      if (options.AllowedOrigins is not null && options.AllowedOrigins.Length > 0)
      {
         if (string.IsNullOrEmpty(origin))
         {
            TraceLogger.LogServerError("WS Handshake: Server handshake failed: Origin header is missing but AllowedOrigins is configured.");
            await SendErrorResponseAsync(writer, "400 Bad Request", "Origin header is required.");
            return (null, requestHeaders, requestCookies);
         }

         var matched = false;
         foreach (var allowed in options.AllowedOrigins)
         {
            if (string.Equals(origin, allowed, StringComparison.OrdinalIgnoreCase))
            {
               matched = true;
               break;
            }
         }

         if (!matched)
         {
            TraceLogger.LogServerError("WS Handshake: Server handshake failed: origin '{0}' is not allowed.", origin);
            await SendErrorResponseAsync(writer, "403 Forbidden", "Origin is not allowed.");
            return (null, requestHeaders, requestCookies);
         }
      }

      if (!isUpgrade || !isConnectionUpgrade || string.IsNullOrEmpty(clientKey) || clientKey.Length > 128)
      {
         TraceLogger.LogServerError("WS Handshake: Server handshake failed: missing, invalid, or too long WebSocket upgrade headers.");
         await SendErrorResponseAsync(writer, "400 Bad Request", "Invalid WebSocket upgrade headers.");
         return (null, requestHeaders, requestCookies);
      }

      // Complete handshake
      var acceptKey = ComputeAcceptKey(clientKey);
      {
         var response = new TextWriterIndentSlim(stackalloc char[512], stackalloc char[1]);
         try
         {
            response.Write("HTTP/1.1 101 Switching Protocols\r\n");
            response.Write("Upgrade: websocket\r\n");
            response.Write("Connection: Upgrade\r\n");
            response.Write("Sec-WebSocket-Accept: ");
            response.Write(acceptKey);
            response.Write("\r\n");

            if (!string.IsNullOrEmpty(options.Subprotocol))
            {
               response.Write("Sec-WebSocket-Protocol: ");
               response.Write(options.Subprotocol);
               response.Write("\r\n");
            }

            response.Write("\r\n");

            var writtenSpan = response.WrittenSpan;
            var maxByteCount = Encoding.ASCII.GetByteCount(writtenSpan);

            var byteSpan = writer.GetSpan(maxByteCount);
            var bytesWritten = Encoding.ASCII.GetBytes(writtenSpan, byteSpan);

            writer.Advance(bytesWritten);
         }
         finally
         {
            response.Dispose();
         }
      }

      await writer.FlushAsync(ct);

      TraceLogger.LogServerInfo("WS Handshake: Server WebSocket upgrade handshake successful (Accept Key: {0})", acceptKey);
      return (acceptKey, requestHeaders, requestCookies);
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

      var randomBytes = new byte[16];
      RandomNumberGenerator.Fill(randomBytes);

      var secWebSocketKey = Convert.ToBase64String(randomBytes);
      var expectedAcceptKey = ComputeAcceptKey(secWebSocketKey);

      var host = endPoint.ToString() ?? "localhost";
      TraceLogger.LogClientInfo("WS Handshake: Starting client WebSocket handshake with host {0} on path {1}", host, options.Path);

      {
         var request = new TextWriterIndentSlim(stackalloc char[1024], stackalloc char[1]);
         try
         {
            request.Write("GET ");
            request.Write(options.Path);
            request.Write(" HTTP/1.1\r\n");
            request.Write("Host: ");
            request.Write(host);
            request.Write("\r\n");
            request.Write("Upgrade: websocket\r\n");
            request.Write("Connection: Upgrade\r\n");
            request.Write("Sec-WebSocket-Key: ");
            request.Write(secWebSocketKey);
            request.Write("\r\n");
            request.Write("Sec-WebSocket-Version: 13\r\n");

            if (!string.IsNullOrEmpty(options.Subprotocol))
            {
               request.Write("Sec-WebSocket-Protocol: ");
               request.Write(options.Subprotocol);
               request.Write("\r\n");
            }
            if (!string.IsNullOrEmpty(options.Origin))
            {
               request.Write("Origin: ");
               request.Write(options.Origin);
               request.Write("\r\n");
            }

            if (options.Headers is not null)
            {
               foreach (var (key, value) in options.Headers)
               {
                  request.Write(key);
                  request.Write(": ");
                  request.Write(value);
                  request.Write("\r\n");
               }
            }

            if (options.Cookies is not null && options.Cookies.Count > 0)
            {
               request.Write("Cookie: ");
               var first = true;
               foreach (var (name, value) in options.Cookies)
               {
                  if (!first)
                  {
                     request.Write("; ");
                  }
                  request.Write(name);
                  request.Write("=");
                  request.Write(value);
                  first = false;
               }
               request.Write("\r\n");
            }

            request.Write("\r\n");

            var writtenSpan = request.WrittenSpan;
            var maxByteCount = Encoding.ASCII.GetByteCount(writtenSpan);

            var byteSpan = writer.GetSpan(maxByteCount);
            var bytesWritten = Encoding.ASCII.GetBytes(writtenSpan, byteSpan);

            writer.Advance(bytesWritten);
         }
         finally
         {
            request.Dispose();
         }
      }

      await writer.FlushAsync(ct);

      // Read response headers
      var responseHeaders = await ReadHttpHeadersAsync(reader, options.MaxHeaderSize, ct);
      if (responseHeaders == null)
      {
         TraceLogger.LogClientError("WS Handshake: Failed to read HTTP response headers from server or headers exceeded limits.");
         return false;
      }

      var lines = responseHeaders.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
      if (lines.Length == 0 || !lines[0].Contains("101"))
      {
         TraceLogger.LogClientError("WS Handshake: Client handshake failed: expected status code 101 Switching Protocols.");
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

      if (!serverAcceptKeyMatched)
      {
         TraceLogger.LogClientError("WS Handshake: Client handshake failed: server accept key validation failed.");
      }
      else
      {
         TraceLogger.LogClientInfo("WS Handshake: Client WebSocket handshake successfully completed (Expected Accept Key: {0})", expectedAcceptKey);
      }

      return serverAcceptKeyMatched;
   }

   private static async Task<string?> ReadHttpHeadersAsync(PipeReader reader, int maxHeaderSize, CancellationToken ct)
   {
      while (true)
      {
         var result = await reader.ReadAsync(ct);
         var buffer = result.Buffer;

         var position = FindSequence(buffer, EndOfHeadersSequence);
         if (position.HasValue)
         {
            var headerSequence = buffer.Slice(0, position.Value);
            if (headerSequence.Length > maxHeaderSize)
            {
               TraceLogger.LogServerError("WS Handshake: HTTP headers exceeded the maximum allowed size of {0} bytes.", maxHeaderSize);
               reader.AdvanceTo(buffer.End);

               return null;
            }

            var headerText = Encoding.ASCII.GetString(headerSequence);

            reader.AdvanceTo(buffer.GetPosition(4, position.Value));
            return headerText;
         }

         if (buffer.Length > maxHeaderSize)
         {
            TraceLogger.LogServerError("WS Handshake: HTTP headers exceeded the maximum allowed size of {0} bytes without reaching end of headers.", maxHeaderSize);
            reader.AdvanceTo(buffer.End);

            return null;
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

       var reader = new SequenceReader<byte>(buffer);
       while (reader.TryAdvanceTo(sequence[0], advancePastDelimiter: false))
       {
          var startPosition = reader.Position;
          if (reader.Remaining >= sequence.Length)
          {
             var match = true;
             for (var i = 0; i < sequence.Length; i++)
             {
                if (!reader.TryPeek(i, out var b) || b != sequence[i])
                {
                   match = false;
                   break;
                }
             }
             if (match)
             {
                return startPosition;
             }
          }
          reader.Advance(1);
       }
       return null;
    }

   private static async Task SendErrorResponseAsync(PipeWriter writer, string status, string message)
   {
      var totalCharsLength = HttpVersionPrefix.Length + ErrorResponseHeaders.Length + status.Length + message.Length;

      {
         using var charOwner = totalCharsLength < 256
            ? new SpanOwner<char>(stackalloc char[totalCharsLength])
            : new SpanOwner<char>(totalCharsLength);

         var charSpan = charOwner.Span;
         HttpVersionPrefix.AsSpan().CopyTo(charSpan);
         var written = HttpVersionPrefix.Length;

         status.AsSpan().CopyTo(charSpan[written..]);
         written += status.Length;

         ErrorResponseHeaders.AsSpan().CopyTo(charSpan[written..]);
         written += ErrorResponseHeaders.Length;

         message.AsSpan().CopyTo(charSpan[written..]);

         var maxByteCount = Encoding.UTF8.GetByteCount(charSpan);

         using var byteOwner = maxByteCount < 512
            ? new SpanOwner<byte>(stackalloc byte[maxByteCount])
            : new SpanOwner<byte>(maxByteCount);

         var bytesWritten = Encoding.UTF8.GetBytes(charSpan, byteOwner.Span);
         writer.Write(byteOwner.Span[..bytesWritten]);
      }

      await writer.FlushAsync();
   }
}

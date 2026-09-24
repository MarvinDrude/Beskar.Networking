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
   /// Computes the Sec-WebSocket-Accept key response for a given client key directly into the destination span.
   /// </summary>
   public static void ComputeAcceptKey(ReadOnlySpan<byte> secWebSocketKey, Span<byte> destination)
   {
      ArgumentOutOfRangeException.ThrowIfGreaterThan(secWebSocketKey.Length, 128);
      if (secWebSocketKey.IsEmpty)
      {
         throw new ArgumentException("Key cannot be empty.", nameof(secWebSocketKey));
      }

      Span<byte> combined = stackalloc byte[secWebSocketKey.Length + 36];
      secWebSocketKey.CopyTo(combined);
      "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"u8.CopyTo(combined[secWebSocketKey.Length..]);

      Span<byte> hash = stackalloc byte[20];
      SHA1.HashData(combined, hash);

      System.Buffers.Text.Base64.EncodeToUtf8(hash, destination, out _, out _);
   }

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

      Span<byte> keyBytes = stackalloc byte[secWebSocketKey.Length];
      Encoding.ASCII.GetBytes(secWebSocketKey, keyBytes);

      Span<byte> acceptKeyBytes = stackalloc byte[28];
      ComputeAcceptKey(keyBytes, acceptKeyBytes);

      return Encoding.ASCII.GetString(acceptKeyBytes);
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

      while (true)
      {
         var readResult = await reader.ReadAsync(ct);
         var buffer = readResult.Buffer;

         var position = FindSequence(buffer, EndOfHeadersSequence);
         if (position.HasValue)
         {
            var headerSequence = buffer.Slice(0, position.Value);
            if (headerSequence.Length > options.MaxHeaderSize)
            {
               TraceLogger.LogServerError("WS Handshake: HTTP headers exceeded the maximum allowed size of {0} bytes.", options.MaxHeaderSize);
               reader.AdvanceTo(buffer.End);
               return (null, null, null);
            }

            byte[]? rented = null;
            ReadOnlySpan<byte> headerSpan;

            if (headerSequence.IsSingleSegment)
            {
               headerSpan = headerSequence.FirstSpan;
            }
            else
            {
               rented = ArrayPool<byte>.Shared.Rent((int)headerSequence.Length);
               headerSequence.CopyTo(rented);
               headerSpan = rented.AsSpan(0, (int)headerSequence.Length);
            }

            try
            {
               var remaining = headerSpan;

               // Parse the first line (GET /path HTTP/1.1)
               var firstLineEnd = remaining.IndexOf("\r\n"u8);
               ReadOnlySpan<byte> firstLine;
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

               if (!firstLine.StartsWith("GET "u8) && !firstLine.StartsWith("get "u8))
               {
                  TraceLogger.LogServerError("WS Handshake: Server handshake failed: only GET requests are allowed.");
                  reader.AdvanceTo(buffer.GetPosition(4, position.Value));
                  await SendErrorResponseAsync(writer, "400 Bad Request", "Only GET requests are allowed.");
                  return (null, null, null);
               }

               var firstSpace = firstLine.IndexOf((byte)' ');
               if (firstSpace == -1)
               {
                  TraceLogger.LogServerError("WS Handshake: Server handshake failed: invalid GET request format.");
                  reader.AdvanceTo(buffer.GetPosition(4, position.Value));
                  return (null, null, null);
               }

               var afterGet = firstLine[(firstSpace + 1)..];
               var secondSpace = afterGet.IndexOf((byte)' ');
               if (secondSpace == -1)
               {
                  TraceLogger.LogServerError("WS Handshake: Server handshake failed: invalid GET request format.");
                  reader.AdvanceTo(buffer.GetPosition(4, position.Value));
                  return (null, null, null);
               }

               var pathSpan = afterGet[..secondSpace];
               if (!Ascii.Equals(pathSpan, options.Path.AsSpan()))
               {
                  TraceLogger.LogServerError("WS Handshake: Server handshake failed: specified path does not match expected path.");
                  reader.AdvanceTo(buffer.GetPosition(4, position.Value));
                  await SendErrorResponseAsync(writer, "404 Not Found", "Specified path is not found.");
                  return (null, null, null);
               }

               ReadOnlySpan<byte> clientKey = default;
               var isUpgrade = false;
               var isConnectionUpgrade = false;
               ReadOnlySpan<byte> origin = default;

               Dictionary<string, string>? requestHeaders = null;
               Dictionary<string, string>? requestCookies = null;

               while (!remaining.IsEmpty)
               {
                  var lineEnd = remaining.IndexOf("\r\n"u8);
                  ReadOnlySpan<byte> line;
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

                  var colonIdx = line.IndexOf((byte)':');
                  if (colonIdx == -1) continue;

                  var headerNameSpan = line[..colonIdx].Trim((byte)' ');
                  var headerValueSpan = line[(colonIdx + 1)..].Trim((byte)' ');

                  if (options.GatherHeaders)
                  {
                     requestHeaders ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                     requestHeaders[Encoding.ASCII.GetString(headerNameSpan)] = Encoding.UTF8.GetString(headerValueSpan);
                  }

                  if (Ascii.EqualsIgnoreCase(headerNameSpan, "upgrade"u8))
                  {
                     if (Ascii.EqualsIgnoreCase(headerValueSpan, "websocket"u8))
                     {
                        isUpgrade = true;
                     }
                  }
                  else if (Ascii.EqualsIgnoreCase(headerNameSpan, "connection"u8))
                  {
                     if (ContainsUpgrade(headerValueSpan))
                     {
                        isConnectionUpgrade = true;
                     }
                  }
                  else if (Ascii.EqualsIgnoreCase(headerNameSpan, "sec-websocket-key"u8))
                  {
                     clientKey = headerValueSpan;
                  }
                  else if (Ascii.EqualsIgnoreCase(headerNameSpan, "origin"u8))
                  {
                     origin = headerValueSpan;
                  }
                  else if (Ascii.EqualsIgnoreCase(headerNameSpan, "cookie"u8) && options.GatherCookies)
                  {
                     requestCookies ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                     var cookieRemaining = headerValueSpan;
                     while (!cookieRemaining.IsEmpty)
                     {
                        var semiIdx = cookieRemaining.IndexOf((byte)';');
                        ReadOnlySpan<byte> cookiePair;

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

                        cookiePair = cookiePair.Trim((byte)' ');
                        if (cookiePair.IsEmpty) continue;

                        var eqIdx = cookiePair.IndexOf((byte)'=');
                        if (eqIdx != -1)
                        {
                           var nameSpan = cookiePair[..eqIdx].Trim((byte)' ');
                           var valueSpan = cookiePair[(eqIdx + 1)..].Trim((byte)' ');
                           requestCookies[Encoding.ASCII.GetString(nameSpan)] = Encoding.UTF8.GetString(valueSpan);
                        }
                     }
                  }
               }

               if (options.AllowedOrigins is not null && options.AllowedOrigins.Length > 0)
               {
                  if (origin.IsEmpty)
                  {
                     TraceLogger.LogServerError("WS Handshake: Server handshake failed: Origin header is missing but AllowedOrigins is configured.");
                     reader.AdvanceTo(buffer.GetPosition(4, position.Value));
                     await SendErrorResponseAsync(writer, "400 Bad Request", "Origin header is required.");
                     return (null, requestHeaders, requestCookies);
                  }

                  var matched = false;
                  foreach (var allowed in options.AllowedOrigins)
                  {
                     if (Ascii.EqualsIgnoreCase(origin, allowed.AsSpan()))
                     {
                        matched = true;
                        break;
                     }
                  }

                  if (!matched)
                  {
                     TraceLogger.LogServerError("WS Handshake: Server handshake failed: origin '{0}' is not allowed.", Encoding.UTF8.GetString(origin));
                     reader.AdvanceTo(buffer.GetPosition(4, position.Value));
                     await SendErrorResponseAsync(writer, "403 Forbidden", "Origin is not allowed.");
                     return (null, requestHeaders, requestCookies);
                  }
               }

               if (!isUpgrade || !isConnectionUpgrade || clientKey.IsEmpty || clientKey.Length > 128)
               {
                  TraceLogger.LogServerError("WS Handshake: Server handshake failed: missing, invalid, or too long WebSocket upgrade headers.");
                  reader.AdvanceTo(buffer.GetPosition(4, position.Value));
                  await SendErrorResponseAsync(writer, "400 Bad Request", "Invalid WebSocket upgrade headers.");
                  return (null, requestHeaders, requestCookies);
               }

               // Advance past headers
               reader.AdvanceTo(buffer.GetPosition(4, position.Value));

               // Complete handshake
               var acceptKey = WriteHandshakeResponse(writer, clientKey, options);

               if (rented != null)
               {
                  ArrayPool<byte>.Shared.Return(rented);
                  rented = null;
               }

               await writer.FlushAsync(ct);

               TraceLogger.LogServerInfo("WS Handshake: Server WebSocket upgrade handshake successful (Accept Key: {0})", acceptKey);
               return (acceptKey, requestHeaders, requestCookies);
            }
            finally
            {
               if (rented != null)
               {
                  ArrayPool<byte>.Shared.Return(rented);
               }
            }
         }

         if (buffer.Length > options.MaxHeaderSize)
         {
            TraceLogger.LogServerError("WS Handshake: HTTP headers exceeded the maximum allowed size of {0} bytes without reaching end of headers.", options.MaxHeaderSize);
            reader.AdvanceTo(buffer.End);
            return (null, null, null);
         }

         reader.AdvanceTo(buffer.Start, buffer.End);

         if (readResult.IsCompleted || readResult.IsCanceled)
         {
            return (null, null, null);
         }
      }
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

   private static bool ContainsUpgrade(ReadOnlySpan<byte> span)
   {
      if (Ascii.EqualsIgnoreCase(span, "upgrade"u8))
         return true;

      var needle = "upgrade"u8;

      if (span.Length < needle.Length)
         return false;

      for (var i = 0; i <= span.Length - needle.Length; i++)
      {
         if (Ascii.EqualsIgnoreCase(span.Slice(i, needle.Length), needle))
         {
            return true;
         }
      }

      return false;
   }

   private static string WriteHandshakeResponse(PipeWriter writer, ReadOnlySpan<byte> clientKey, WsTransportOptions options)
   {
      Span<byte> acceptKeyBytes = stackalloc byte[28];
      ComputeAcceptKey(clientKey, acceptKeyBytes);

      var span = writer.GetSpan(256);
      var written = 0;
      var prefix = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "u8;
      prefix.CopyTo(span);
      written += prefix.Length;

      acceptKeyBytes.CopyTo(span[written..]);
      written += acceptKeyBytes.Length;

      if (!string.IsNullOrEmpty(options.Subprotocol))
      {
         var subPrefix = "\r\nSec-WebSocket-Protocol: "u8;
         subPrefix.CopyTo(span[written..]);
         written += subPrefix.Length;
         written += Encoding.UTF8.GetBytes(options.Subprotocol, span[written..]);
      }

      var suffix = "\r\n\r\n"u8;
      suffix.CopyTo(span[written..]);
      written += suffix.Length;

      writer.Advance(written);
      return Encoding.ASCII.GetString(acceptKeyBytes);
   }
}

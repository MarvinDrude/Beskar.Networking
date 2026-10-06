using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Extensions;

internal static class NetworkStreamExtensions
{
   extension(INetworkStream stream)
   {
      internal Task Send<TPacket>(in TPacket packet, MqttProtocolVersion protocolVersion, CancellationToken ct = default)
         where TPacket : IRawMqttPacket
      {
         return SendAsync(stream, packet, protocolVersion, ct);
      }

      private static async Task SendAsync<TPacket>(INetworkStream targetStream, TPacket packet, MqttProtocolVersion protocolVersion, CancellationToken ct)
         where TPacket : IRawMqttPacket
      {
         TraceLogger.LogServerInfo("NetworkStreamExtensions.Send: Sending packet '{0}'...", typeof(TPacket).Name);
         try
         {
            using (await targetStream.AcquireWriterLock(ct).ConfigureAwait(false))
            {
               var writer = targetStream.Transport.Output;
               switch (protocolVersion)
               {
                  case MqttProtocolVersion.V50:
                     new PacketVersion5Encoder(writer).Write(in packet);
                     break;
                  case MqttProtocolVersion.V31:
                  case MqttProtocolVersion.V311:
                     new PacketVersion3Encoder(writer, protocolVersion).Write(in packet);
                     break;
                  default:
                     throw new InvalidOperationException("Unknown protocol version.");
               }

               await writer.FlushAsync(ct).ConfigureAwait(false);
            }
         }
         catch (Exception error)
         {
            TraceLogger.LogServerError("NetworkStreamExtensions.Send: Error writing packet '{0}': {1}", typeof(TPacket).Name, error.Message);
            throw;
         }
      }

      internal Task Send<TOptions>(TOptions options, MqttProtocolVersion protocolVersion, CancellationToken ct = default)
         where TOptions : class, IHeapMqttOptions
      {
         return stream.Send(options, protocolVersion, 0, ct);
      }

      internal async Task Send<TOptions>(TOptions options, MqttProtocolVersion protocolVersion, ushort identifier, CancellationToken ct = default)
         where TOptions : class, IHeapMqttOptions
      {
         TraceLogger.LogServerInfo("NetworkStreamExtensions.Send: Sending options '{0}' (PacketId: {1})...", typeof(TOptions).Name, identifier);
         try
         {
            using (await stream.AcquireWriterLock(ct).ConfigureAwait(false))
            {
               var writer = stream.Transport.Output;
               switch (protocolVersion)
               {
                  case MqttProtocolVersion.V50:
                     new PacketVersion5Encoder(writer).Write(options, identifier);
                     break;
                  case MqttProtocolVersion.V31:
                  case MqttProtocolVersion.V311:
                     new PacketVersion3Encoder(writer, protocolVersion).Write(options, identifier);
                     break;
                  default:
                     throw new InvalidOperationException("Unknown protocol version.");
               }

               await writer.FlushAsync(ct).ConfigureAwait(false);
            }
         }
         catch (Exception error)
         {
            TraceLogger.LogServerError("NetworkStreamExtensions.Send: Error writing options '{0}': {1}", typeof(TOptions).Name, error.Message);
            throw;
         }
      }
   }
}

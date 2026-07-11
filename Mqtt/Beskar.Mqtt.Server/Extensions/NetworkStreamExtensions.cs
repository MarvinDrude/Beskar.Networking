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
         TraceLogger.LogServerInfo("NetworkStreamExtensions.Send: Sending packet '{0}'...", typeof(TPacket).Name);
         try
         {
            var lockTask = stream.AcquireWriterLock(ct);
            if (!lockTask.IsCompletedSuccessfully)
               return stream.SendSlowAsync(packet, protocolVersion, lockTask, ct);

            var lockToken = lockTask.Result;
            try
            {
               var writer = stream.Transport.Output;
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

               var flushTask = writer.FlushAsync(ct);
               if (!flushTask.IsCompletedSuccessfully)
                  return stream.CompleteFlushAsync(flushTask, lockToken);

               // consume flush task
               _ = flushTask.Result.IsCompleted;

               lockToken.Dispose();
               return Task.CompletedTask;
            }
            catch (Exception error)
            {
               TraceLogger.LogServerError("NetworkStreamExtensions.Send: Error writing packet '{0}': {1}", typeof(TPacket).Name, error.Message);
               lockToken.Dispose();
               return Task.FromException(error);
            }
         }
         catch (Exception error)
         {
            TraceLogger.LogServerError("NetworkStreamExtensions.Send: Error acquiring writer lock for '{0}': {1}", typeof(TPacket).Name, error.Message);
            return Task.FromException(error);
         }
      }

      internal Task Send<TOptions>(TOptions options, MqttProtocolVersion protocolVersion, CancellationToken ct = default)
         where TOptions : class, IHeapMqttOptions
      {
         return stream.Send(options, protocolVersion, 0, ct);
      }

      internal Task Send<TOptions>(TOptions options, MqttProtocolVersion protocolVersion, ushort identifier, CancellationToken ct = default)
         where TOptions : class, IHeapMqttOptions
      {
         TraceLogger.LogServerInfo("NetworkStreamExtensions.Send: Sending options '{0}' (PacketId: {1})...", typeof(TOptions).Name, identifier);
         try
         {
            var lockTask = stream.AcquireWriterLock(ct);
            if (!lockTask.IsCompletedSuccessfully)
               return stream.SendSlowAsync(options, protocolVersion, identifier, lockTask, ct);

            var lockToken = lockTask.Result;
            try
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

               var flushTask = writer.FlushAsync(ct);
               if (!flushTask.IsCompletedSuccessfully)
                  return stream.CompleteFlushAsync(flushTask, lockToken);

               // consume flush task
               _ = flushTask.Result.IsCompleted;

               lockToken.Dispose();
               return Task.CompletedTask;
            }
            catch (Exception error)
            {
               TraceLogger.LogServerError("NetworkStreamExtensions.Send: Error writing options '{0}': {1}", typeof(TOptions).Name, error.Message);
               lockToken.Dispose();
               return Task.FromException(error);
            }
         }
         catch (Exception error)
         {
            TraceLogger.LogServerError("NetworkStreamExtensions.Send: Error acquiring writer lock for options '{0}': {1}", typeof(TOptions).Name, error.Message);
            return Task.FromException(error);
         }
      }

      private async Task CompleteFlushAsync(
         ValueTask<FlushResult> flushTask,
         LockReleaser lockToken)
      {
         using (lockToken)
         {
            await flushTask;
         }
      }

      private async Task SendSlowAsync<TPacket>(
         TPacket packet,
         MqttProtocolVersion protocolVersion,
         ValueTask<LockReleaser> lockTask,
         CancellationToken ct)
         where TPacket : IRawMqttPacket
      {
         using (await lockTask)
         {
            var writer = stream.Transport.Output;
            switch (protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).Write(packet);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, protocolVersion).Write(packet);
                  break;
               default:
                  throw new InvalidOperationException("Unknown protocol version.");
            }

            await writer.FlushAsync(ct);
         }
      }

      private async Task SendSlowAsync<TOptions>(
         TOptions options,
         MqttProtocolVersion protocolVersion,
         ushort identifier,
         ValueTask<LockReleaser> lockTask,
         CancellationToken ct)
         where TOptions : class, IHeapMqttOptions
      {
         using (await lockTask)
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

            await writer.FlushAsync(ct);
         }
      }
   }
}

using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.ChaosSimulator;

public sealed class AuthHandler(bool solveCorrectly) : IMqttAuthenticationHandler
{
   private readonly bool _solveCorrectly = solveCorrectly;

   public async Task<VoidResult<StringError>> ExecuteAsync(
      MqttAuthContext context, CancellationToken ct = default)
   {
      var authPacket = context.AuthPacket;
      if (authPacket.ReasonCode == AuthenticateReasonCode.ContinueAuthentication)
      {
         var challengeBytes = authPacket.AuthenticationData?.ToArray();
         if (challengeBytes is not null)
         {
            var responseBytes = new byte[challengeBytes.Length];
            for (var i = 0; i < challengeBytes.Length; i++)
               responseBytes[i] = (byte)(challengeBytes[i] + (_solveCorrectly ? 1 : 0));
            await context.SendResponseAsync(responseBytes, _solveCorrectly ? "Challenge solved" : "Challenge failed",
               ct);
         }
      }

      return true;
   }
}

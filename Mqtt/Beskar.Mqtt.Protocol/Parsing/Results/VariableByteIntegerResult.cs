namespace Beskar.Mqtt.Protocol.Parsing.Results;

public enum VariableByteIntegerResult
{
   Success = 1,
   ExceedMaxValue = 2,
   NotEnoughData = 3
}

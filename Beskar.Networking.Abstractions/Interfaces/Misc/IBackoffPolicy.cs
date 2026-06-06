namespace Beskar.Networking.Abstractions.Interfaces.Misc;

public interface IBackoffPolicy
{
   public TimeSpan GetNextDelay(int attempt);
}

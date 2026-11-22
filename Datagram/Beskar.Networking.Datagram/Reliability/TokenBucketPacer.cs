namespace Beskar.Networking.Datagram.Reliability;

/// <summary>
/// Token-bucket byte pacer preventing burst congestion on per-peer outbound sends.
/// </summary>
internal sealed class TokenBucketPacer
{
   private readonly long? _maxBytesPerSecond;
   private readonly double _burstBytes;
   private double _availableTokens;
   private long _lastRefillTimestamp;

   public TokenBucketPacer(long? maxBytesPerSecond, int burstBytes, long nowTimestamp)
   {
      _maxBytesPerSecond = maxBytesPerSecond;
      _burstBytes = Math.Max(2048, burstBytes);
      _availableTokens = _burstBytes;
      _lastRefillTimestamp = nowTimestamp;
   }

   public bool CanSend(int byteCount, long nowTimestamp, TimeProvider timeProvider)
   {
      if (!_maxBytesPerSecond.HasValue)
      {
         return true;
      }

      Refill(nowTimestamp, timeProvider);
      return _availableTokens >= byteCount || _availableTokens >= _burstBytes * 0.5;
   }

   public void Consume(int byteCount, long nowTimestamp, TimeProvider timeProvider)
   {
      if (!_maxBytesPerSecond.HasValue)
      {
         return;
      }

      Refill(nowTimestamp, timeProvider);
      _availableTokens = Math.Max(-_burstBytes, _availableTokens - byteCount);
   }

   private void Refill(long nowTimestamp, TimeProvider timeProvider)
   {
      var elapsed = nowTimestamp - _lastRefillTimestamp;
      if (elapsed <= 0)
      {
         return;
      }

      var seconds = (double)elapsed / timeProvider.TimestampFrequency;
      _availableTokens = Math.Min(_burstBytes, _availableTokens + seconds * _maxBytesPerSecond!.Value);
      _lastRefillTimestamp = nowTimestamp;
   }
}

using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Telemetry;

namespace Beskar.Networking.Datagram.Reliability;

/// <summary>
/// Implements Jacobson/Karels (RFC 6298) RTT/RTO estimation, rolling packet-loss tracking,
/// congestion signaling, and NTP-style clock-offset smoothing.
/// </summary>
internal sealed class RttEstimator(
   double minRtoMs,
   double maxRtoMs,
   TimeSpan tickInterval,
   TimeProvider timeProvider,
   long nowTimestamp)
{
   private readonly double _minRtoMs = minRtoMs;
   private readonly double _maxRtoMs = maxRtoMs;
   private readonly long _tickDurationTicks = Math.Max(1, tickInterval.Ticks);
   private readonly long _windowDurationTicks = timeProvider.TimestampFrequency; // 1 second

   private bool _hasSample;
   private double _baselineRttMs = 100.0;
   private long _windowStartTimestamp = nowTimestamp;
   private int _windowAcked;
   private int _windowLost;

   private bool _hasClockOffset;

   public double SmoothedRttMs { get; private set; } = 100.0;
   public double RttVarianceMs { get; private set; } = 50.0;
   public double CurrentRtoMs { get; private set; } = 250.0;
   public double PacketLossRate { get; private set; }
   public DatagramCongestionState CongestionState { get; private set; } = DatagramCongestionState.Healthy;
   public float RecommendedSendScale { get; private set; } = 1.0f;
   public long EstimatedClockOffsetTicks { get; private set; }

   public void RecordRttSample(double sampleMs)
   {
      if (sampleMs < 0.0) sampleMs = 0.0;

      if (!_hasSample)
      {
         _hasSample = true;

         SmoothedRttMs = sampleMs;
         RttVarianceMs = sampleMs * 0.5;

         _baselineRttMs = Math.Max(1.0, sampleMs);
      }
      else
      {
         var diff = Math.Abs(SmoothedRttMs - sampleMs);
         RttVarianceMs = 0.75 * RttVarianceMs + 0.25 * diff;
         SmoothedRttMs = 0.875 * SmoothedRttMs + 0.125 * sampleMs;

         if (sampleMs < _baselineRttMs)
         {
            _baselineRttMs = Math.Max(1.0, sampleMs);
         }
         else
         {
            _baselineRttMs = 0.995 * _baselineRttMs + 0.005 * sampleMs;
         }
      }

      var tickMs = (double)_tickDurationTicks / TimeSpan.TicksPerMillisecond;
      var rawRto = SmoothedRttMs + Math.Max(tickMs, 4.0 * RttVarianceMs);
      CurrentRtoMs = Math.Clamp(rawRto, _minRtoMs, _maxRtoMs);

      DatagramMetrics.RoundTripTimeMs.Record(sampleMs);
   }

   public void RecordPacketAcked(long nowTimestamp, TimeProvider timeProvider)
   {
      _windowAcked++;
      MaybeRollLossWindow(nowTimestamp, timeProvider);
   }

   public void RecordPacketLost(long nowTimestamp, TimeProvider timeProvider)
   {
      _windowLost++;
      MaybeRollLossWindow(nowTimestamp, timeProvider);
   }

   public void RecordTimeSyncSample(long clientSendUtcTicks, long serverRecvUtcTicks, long serverSendUtcTicks, long clientRecvUtcTicks)
   {
      var rawOffset = ((serverRecvUtcTicks - clientSendUtcTicks) + (serverSendUtcTicks - clientRecvUtcTicks)) / 2;
      if (!_hasClockOffset)
      {
         _hasClockOffset = true;
         EstimatedClockOffsetTicks = rawOffset;
      }
      else
      {
         EstimatedClockOffsetTicks = (long)(0.85 * EstimatedClockOffsetTicks + 0.15 * rawOffset);
      }
   }

   public long GetCurrentRtoTicks(TimeProvider timeProvider)
   {
      return (long)(CurrentRtoMs * timeProvider.TimestampFrequency / 1000.0);
   }

   public void UpdateLossWindow(long nowTimestamp, TimeProvider timeProvider)
   {
      MaybeRollLossWindow(nowTimestamp, timeProvider);
   }

   private void MaybeRollLossWindow(long nowTimestamp, TimeProvider timeProvider)
   {
      var elapsed = nowTimestamp - _windowStartTimestamp;
      if (elapsed < _windowDurationTicks && _windowAcked + _windowLost < 32)
      {
         return;
      }

      var total = _windowAcked + _windowLost;
      if (total > 0)
      {
         var instantLoss = (double)_windowLost / total;
         PacketLossRate = 0.6 * PacketLossRate + 0.4 * instantLoss;
      }

      _windowAcked = 0;
      _windowLost = 0;
      _windowStartTimestamp = nowTimestamp;

      var rttInflated = _hasSample && SmoothedRttMs > Math.Max(40.0, _baselineRttMs * 2.0);

      if (PacketLossRate >= 0.12)
      {
         CongestionState = DatagramCongestionState.Critical;
         RecommendedSendScale = 0.25f;
      }
      else if (PacketLossRate >= 0.03 || rttInflated)
      {
         CongestionState = DatagramCongestionState.Congested;
         RecommendedSendScale = 0.5f;
      }
      else
      {
         CongestionState = DatagramCongestionState.Healthy;
         RecommendedSendScale = 1.0f;
      }
   }
}

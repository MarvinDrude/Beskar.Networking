using System.Globalization;
using System.Numerics;

namespace Beskar.Jobs.Common.Scheduling;

/// <summary>
/// High-performance, zero-allocation bitmask Cron schedule and interval evaluator.
/// Supports 5-field (minute..dayOfWeek), 6-field (second..dayOfWeek), standard macros (@daily, @hourly, etc.),
/// and sub-second/custom intervals (@every 250ms, @every 5s, @every 10m).
/// </summary>
public readonly struct CronSchedule : IEquatable<CronSchedule>
{
   private const ulong AllSecondsMask = (1UL << 60) - 1UL;
   private const ulong AllMinutesMask = (1UL << 60) - 1UL;
   private const uint AllHoursMask = (1U << 24) - 1U;
   private const uint AllDaysOfMonthMask = ((1U << 31) - 1U) << 1; // bits 1..31
   private const ushort AllMonthsMask = (ushort)(((1U << 12) - 1U) << 1); // bits 1..12
   private const byte AllDaysOfWeekMask = (1 << 7) - 1; // bits 0..6

   /// <summary>
   /// Bitmask of valid seconds (bits 0..59).
   /// </summary>
   public ulong SecondsMask { get; }

   /// <summary>
   /// Bitmask of valid minutes (bits 0..59).
   /// </summary>
   public ulong MinutesMask { get; }

   /// <summary>
   /// Bitmask of valid hours (bits 0..23).
   /// </summary>
   public uint HoursMask { get; }

   /// <summary>
   /// Bitmask of valid days of month (bits 1..31).
   /// </summary>
   public uint DaysOfMonthMask { get; }

   /// <summary>
   /// Bitmask of valid months (bits 1..12).
   /// </summary>
   public ushort MonthsMask { get; }

   /// <summary>
   /// Bitmask of valid days of week (bits 0..6, where 0 = Sunday).
   /// </summary>
   public byte DaysOfWeekMask { get; }

   /// <summary>
   /// Optional fixed interval when parsed from an <c>@every &lt;duration&gt;</c> expression.
   /// </summary>
   public TimeSpan? Interval { get; }

   /// <summary>
   /// True if Day-of-Month was specified as wildcard (<c>*</c> or <c>?</c>).
   /// </summary>
   public bool IsDayOfMonthWildcard { get; }

   /// <summary>
   /// True if Day-of-Week was specified as wildcard (<c>*</c> or <c>?</c>).
   /// </summary>
   public bool IsDayOfWeekWildcard { get; }

   /// <summary>
   /// Returns true if this schedule is a fixed-interval (<c>@every</c>) schedule rather than a calendar cron.
   /// </summary>
   public bool IsInterval => Interval.HasValue;

   public CronSchedule(
      ulong secondsMask,
      ulong minutesMask,
      uint hoursMask,
      uint daysOfMonthMask,
      ushort monthsMask,
      byte daysOfWeekMask,
      bool isDayOfMonthWildcard = true,
      bool isDayOfWeekWildcard = true)
   {
      SecondsMask = secondsMask;
      MinutesMask = minutesMask;
      HoursMask = hoursMask;
      DaysOfMonthMask = daysOfMonthMask;
      MonthsMask = monthsMask;
      DaysOfWeekMask = daysOfWeekMask;
      IsDayOfMonthWildcard = isDayOfMonthWildcard;
      IsDayOfWeekWildcard = isDayOfWeekWildcard;
      Interval = null;
   }

   private CronSchedule(TimeSpan interval)
   {
      SecondsMask = AllSecondsMask;
      MinutesMask = AllMinutesMask;
      HoursMask = AllHoursMask;
      DaysOfMonthMask = AllDaysOfMonthMask;
      MonthsMask = AllMonthsMask;
      DaysOfWeekMask = AllDaysOfWeekMask;
      IsDayOfMonthWildcard = true;
      IsDayOfWeekWildcard = true;
      Interval = interval;
   }

   /// <summary>
   /// Creates a fixed-interval schedule (equivalent to <c>@every &lt;interval&gt;</c>).
   /// </summary>
   public static CronSchedule FromInterval(TimeSpan interval)
   {
      if (interval <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(interval), "Interval must be greater than zero.");
      }

      return new CronSchedule(interval);
   }

   /// <summary>
   /// Parses a 5-field, 6-field, or macro Cron expression. Throws <see cref="FormatException"/> on invalid syntax.
   /// </summary>
   public static CronSchedule Parse(ReadOnlySpan<char> expression)
   {
      if (!TryParse(expression, out var schedule))
      {
         throw new FormatException($"Invalid cron expression: '{expression.ToString()}'.");
      }

      return schedule;
   }

   /// <summary>
   /// Attempts to parse a 5-field, 6-field, or macro Cron expression without heap allocations.
   /// </summary>
   public static bool TryParse(ReadOnlySpan<char> expression, out CronSchedule schedule)
   {
      schedule = default;
      expression = expression.Trim();
      if (expression.IsEmpty)
      {
         return false;
      }

      if (expression[0] == '@')
      {
         return TryParseMacro(expression, out schedule);
      }

      // Split into up to 7 tokens by whitespace
      Span<Range> ranges = stackalloc Range[7];
      var fieldCount = SplitWhitespace(expression, ranges);
      if (fieldCount is not (5 or 6))
      {
         return false;
      }

      var offset = 0;
      var secondsMask = 1UL; // Default second = 0 for 5-field cron

      if (fieldCount == 6)
      {
         if (!TryParseField(expression[ranges[0]], 0, 59, FieldKind.Numeric, out secondsMask, out _))
         {
            return false;
         }

         offset = 1;
      }

      if (!TryParseField(expression[ranges[offset]], 0, 59, FieldKind.Numeric, out var minutesMask, out _))
      {
         return false;
      }

      if (!TryParseField(expression[ranges[offset + 1]], 0, 23, FieldKind.Numeric, out var hoursMask64, out _))
      {
         return false;
      }

      if (!TryParseField(expression[ranges[offset + 2]], 1, 31, FieldKind.Numeric, out var domMask64, out var isDomWildcard))
      {
         return false;
      }

      if (!TryParseField(expression[ranges[offset + 3]], 1, 12, FieldKind.Month, out var monthsMask64, out _))
      {
         return false;
      }

      if (!TryParseField(expression[ranges[offset + 4]], 0, 7, FieldKind.DayOfWeek, out var dowMask64, out var isDowWildcard))
      {
         return false;
      }

      // Map Sunday = 7 to bit 0
      if ((dowMask64 & (1UL << 7)) != 0)
      {
         dowMask64 = (dowMask64 & ~(1UL << 7)) | 1UL;
      }

      schedule = new CronSchedule(
         secondsMask,
         minutesMask,
         (uint)hoursMask64,
         (uint)domMask64,
         (ushort)monthsMask64,
         (byte)dowMask64,
         isDomWildcard,
         isDowWildcard);

      return true;
   }

   /// <summary>
   /// Checks whether the given timestamp matches this Cron schedule (evaluated in UTC).
   /// </summary>
   public bool IsMatch(DateTimeOffset timestamp)
   {
      if (Interval.HasValue)
      {
         return true;
      }

      var utc = timestamp.UtcDateTime;
      if (((SecondsMask >> utc.Second) & 1UL) == 0) return false;
      if (((MinutesMask >> utc.Minute) & 1UL) == 0) return false;
      if (((HoursMask >> utc.Hour) & 1U) == 0) return false;
      if (((MonthsMask >> utc.Month) & 1U) == 0) return false;

      return MatchesDay(utc.Year, utc.Month, utc.Day);
   }

   /// <summary>
   /// Computes the next occurrence of this schedule after (or at, if <paramref name="inclusive"/> is true) <paramref name="from"/>.
   /// Returns <c>null</c> if no matching date exists within the next 5 years.
   /// </summary>
   public DateTimeOffset? GetNextOccurrence(DateTimeOffset from, bool inclusive = false)
   {
      if (Interval is { } interval)
      {
         return inclusive ? from : from.Add(interval);
      }

      if (SecondsMask == 0 || MinutesMask == 0 || HoursMask == 0 || DaysOfMonthMask == 0 || MonthsMask == 0 || DaysOfWeekMask == 0)
      {
         return null;
      }

      var utc = from.UtcDateTime;

      // Truncate sub-second ticks; if not inclusive or if there were sub-second ticks, advance by 1 second
      var truncated = new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, utc.Second, DateTimeKind.Utc);
      if (!inclusive || truncated < utc)
      {
         truncated = truncated.AddSeconds(1);
      }

      var year = truncated.Year;
      var month = truncated.Month;
      var day = truncated.Day;
      var hour = truncated.Hour;
      var minute = truncated.Minute;
      var second = truncated.Second;

      var maxYear = year + 5;

      while (year <= maxYear)
      {
         // 1. Match Month (1..12)
         var nextMonth = NextSetBit32(MonthsMask, month, 12);
         if (nextMonth < 0)
         {
            year++;
            month = 1;
            day = 1;
            hour = 0;
            minute = 0;
            second = 0;
            continue;
         }

         if (nextMonth > month)
         {
            month = nextMonth;
            day = 1;
            hour = 0;
            minute = 0;
            second = 0;
         }

         // 2. Match Day of Month & Day of Week
         var daysInMonth = DateTime.DaysInMonth(year, month);
         if (day > daysInMonth)
         {
            month++;
            if (month > 12)
            {
               year++;
               month = 1;
            }
            day = 1;
            hour = 0;
            minute = 0;
            second = 0;
            continue;
         }

         var matchedDay = -1;
         for (var d = day; d <= daysInMonth; d++)
         {
            if (MatchesDay(year, month, d))
            {
               matchedDay = d;
               break;
            }
         }

         if (matchedDay < 0)
         {
            month++;
            if (month > 12)
            {
               year++;
               month = 1;
            }
            day = 1;
            hour = 0;
            minute = 0;
            second = 0;
            continue;
         }

         if (matchedDay > day)
         {
            day = matchedDay;
            hour = 0;
            minute = 0;
            second = 0;
         }

         // 3. Match Hour (0..23)
         var nextHour = NextSetBit32(HoursMask, hour, 23);
         if (nextHour < 0)
         {
            day++;
            hour = 0;
            minute = 0;
            second = 0;
            continue;
         }

         if (nextHour > hour)
         {
            hour = nextHour;
            minute = 0;
            second = 0;
         }

         // 4. Match Minute (0..59)
         var nextMinute = NextSetBit64(MinutesMask, minute, 59);
         if (nextMinute < 0)
         {
            hour++;
            if (hour > 23)
            {
               day++;
               hour = 0;
            }
            minute = 0;
            second = 0;
            continue;
         }

         if (nextMinute > minute)
         {
            minute = nextMinute;
            second = 0;
         }

         // 5. Match Second (0..59)
         var nextSecond = NextSetBit64(SecondsMask, second, 59);
         if (nextSecond < 0)
         {
            minute++;
            if (minute > 59)
            {
               hour++;
               minute = 0;
               if (hour > 23)
               {
                  day++;
                  hour = 0;
               }
            }
            second = 0;
            continue;
         }

         second = nextSecond;
         return new DateTimeOffset(year, month, day, hour, minute, second, TimeSpan.Zero);
      }

      return null;
   }

   private bool MatchesDay(int year, int month, int day)
   {
      var domMatches = ((DaysOfMonthMask >> day) & 1U) != 0;
      var dow = (int)new DateTime(year, month, day).DayOfWeek;
      var dowMatches = ((DaysOfWeekMask >> dow) & 1) != 0;

      if (!IsDayOfMonthWildcard && !IsDayOfWeekWildcard)
      {
         // Standard Cron semantics: when both DOM and DOW are explicitly restricted, either matching triggers
         return domMatches || dowMatches;
      }

      return domMatches && dowMatches;
   }

   private static int NextSetBit32(uint mask, int startBit, int maxBit)
   {
      if (startBit > maxBit) return -1;
      var shifted = mask & (~0U << startBit);
      if (shifted == 0) return -1;
      var bit = BitOperations.TrailingZeroCount(shifted);
      return bit <= maxBit ? bit : -1;
   }

   private static int NextSetBit64(ulong mask, int startBit, int maxBit)
   {
      if (startBit > maxBit) return -1;
      var shifted = mask & (~0UL << startBit);
      if (shifted == 0) return -1;
      var bit = BitOperations.TrailingZeroCount(shifted);
      return bit <= maxBit ? bit : -1;
   }

   private static bool TryParseMacro(ReadOnlySpan<char> expr, out CronSchedule schedule)
   {
      schedule = default;

      if (expr.Equals("@yearly", StringComparison.OrdinalIgnoreCase) ||
          expr.Equals("@annually", StringComparison.OrdinalIgnoreCase))
      {
         schedule = new CronSchedule(1UL, 1UL, 1U, 1U << 1, 1 << 1, AllDaysOfWeekMask, false, true);
         return true;
      }

      if (expr.Equals("@monthly", StringComparison.OrdinalIgnoreCase))
      {
         schedule = new CronSchedule(1UL, 1UL, 1U, 1U << 1, AllMonthsMask, AllDaysOfWeekMask, false, true);
         return true;
      }

      if (expr.Equals("@weekly", StringComparison.OrdinalIgnoreCase))
      {
         schedule = new CronSchedule(1UL, 1UL, 1U, AllDaysOfMonthMask, AllMonthsMask, 1 << 0, true, false);
         return true;
      }

      if (expr.Equals("@daily", StringComparison.OrdinalIgnoreCase) ||
          expr.Equals("@midnight", StringComparison.OrdinalIgnoreCase))
      {
         schedule = new CronSchedule(1UL, 1UL, 1U, AllDaysOfMonthMask, AllMonthsMask, AllDaysOfWeekMask);
         return true;
      }

      if (expr.Equals("@hourly", StringComparison.OrdinalIgnoreCase))
      {
         schedule = new CronSchedule(1UL, 1UL, AllHoursMask, AllDaysOfMonthMask, AllMonthsMask, AllDaysOfWeekMask);
         return true;
      }

      if (expr.Equals("@minutely", StringComparison.OrdinalIgnoreCase))
      {
         schedule = new CronSchedule(1UL, AllMinutesMask, AllHoursMask, AllDaysOfMonthMask, AllMonthsMask, AllDaysOfWeekMask);
         return true;
      }

      if (expr.Equals("@secondly", StringComparison.OrdinalIgnoreCase))
      {
         schedule = new CronSchedule(AllSecondsMask, AllMinutesMask, AllHoursMask, AllDaysOfMonthMask, AllMonthsMask, AllDaysOfWeekMask);
         return true;
      }

      if (expr.StartsWith("@every ", StringComparison.OrdinalIgnoreCase))
      {
         var durationSpan = expr[7..].Trim();
         if (TryParseDuration(durationSpan, out var interval) && interval > TimeSpan.Zero)
         {
            schedule = new CronSchedule(interval);
            return true;
         }
      }

      return false;
   }

   private static bool TryParseDuration(ReadOnlySpan<char> span, out TimeSpan duration)
   {
      duration = TimeSpan.Zero;
      if (span.IsEmpty) return false;

      if (span.EndsWith("ms", StringComparison.OrdinalIgnoreCase))
      {
         if (!double.TryParse(span[..^2].Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var ms) ||
             !double.IsFinite(ms) ||
             ms <= 0)
         {
            return false;
         }

         duration = TimeSpan.FromMilliseconds(ms);
         return duration > TimeSpan.Zero;
      }

      var unit = char.ToLowerInvariant(span[^1]);
      var numPart = span[..^1].Trim();

      if (!double.TryParse(numPart, NumberStyles.Float, CultureInfo.InvariantCulture, out var val) ||
          !double.IsFinite(val) ||
          val <= 0)
      {
         return TimeSpan.TryParse(span, CultureInfo.InvariantCulture, out duration) && duration > TimeSpan.Zero;
      }

      duration = unit switch
      {
         's' => TimeSpan.FromSeconds(val),
         'm' => TimeSpan.FromMinutes(val),
         'h' => TimeSpan.FromHours(val),
         'd' => TimeSpan.FromDays(val),
         _ => TimeSpan.Zero
      };

      return duration > TimeSpan.Zero;
   }

   private static int SplitWhitespace(ReadOnlySpan<char> input, Span<Range> ranges)
   {
      var count = 0;
      var i = 0;
      while (i < input.Length)
      {
         while (i < input.Length && char.IsWhiteSpace(input[i])) i++;
         if (i >= input.Length) break;

         var start = i;
         while (i < input.Length && !char.IsWhiteSpace(input[i])) i++;

         if (count >= ranges.Length)
         {
            return count + 1;
         }

         ranges[count++] = new Range(start, i);
      }

      return count;
   }

   private enum FieldKind : byte
   {
      Numeric = 0,
      Month = 1,
      DayOfWeek = 2
   }

   private static bool TryParseField(
      ReadOnlySpan<char> field,
      int min,
      int max,
      FieldKind kind,
      out ulong mask,
      out bool isWildcard)
   {
      mask = 0;
      isWildcard = field is "*" or "?";

      if (field.IsEmpty)
      {
         return false;
      }

      var remaining = field;
      while (!remaining.IsEmpty)
      {
         var commaIdx = remaining.IndexOf(',');
         ReadOnlySpan<char> part;
         if (commaIdx >= 0)
         {
            part = remaining[..commaIdx];
            remaining = remaining[(commaIdx + 1)..];
            if (remaining.IsEmpty) return false; // trailing comma
         }
         else
         {
            part = remaining;
            remaining = default;
         }

         if (!TryParsePart(part, min, max, kind, out var partMask))
         {
            return false;
         }

         mask |= partMask;
      }

      return mask != 0;
   }

   private static bool TryParsePart(
      ReadOnlySpan<char> part,
      int min,
      int max,
      FieldKind kind,
      out ulong mask)
   {
      mask = 0;
      if (part.IsEmpty) return false;

      var step = 1;
      var slashIdx = part.IndexOf('/');
      if (slashIdx >= 0)
      {
         var stepSpan = part[(slashIdx + 1)..];
         if (!int.TryParse(stepSpan, NumberStyles.Integer, CultureInfo.InvariantCulture, out step) ||
             step <= 0 ||
             step > max - min + 1)
         {
            return false;
         }

         part = part[..slashIdx];
      }

      int rangeStart;
      int rangeEnd;

      if (part is "*" or "?")
      {
         rangeStart = min;
         rangeEnd = kind == FieldKind.DayOfWeek ? 6 : max;
      }
      else
      {
         var dashIdx = part.IndexOf('-');
         if (dashIdx >= 0)
         {
            if (!TryParseValue(part[..dashIdx], min, max, kind, out rangeStart) ||
                !TryParseValue(part[(dashIdx + 1)..], min, max, kind, out rangeEnd))
            {
               return false;
            }

            if (kind == FieldKind.DayOfWeek && rangeEnd == 0 && rangeStart > 0)
            {
               // Map trailing SUN (0) in ranges like FRI-SUN or MON-SUN to 7
               rangeEnd = 7;
            }

            if (rangeStart > rangeEnd)
            {
               return false;
            }
         }
         else
         {
            if (!TryParseValue(part, min, max, kind, out rangeStart))
            {
               return false;
            }

            if (slashIdx >= 0 && kind == FieldKind.DayOfWeek && rangeStart == 7)
            {
               rangeStart = 0;
            }

            // If a step was given with a single value (e.g. "5/15"), it runs from 5 to max
            rangeEnd = slashIdx >= 0 ? (kind == FieldKind.DayOfWeek ? 6 : max) : rangeStart;
         }
      }

      for (var v = rangeStart; v <= rangeEnd; v += step)
      {
         mask |= 1UL << v;
      }

      return mask != 0;
   }

   private static bool TryParseValue(
      ReadOnlySpan<char> token,
      int min,
      int max,
      FieldKind kind,
      out int value)
   {
      if (int.TryParse(token, out value))
      {
         return value >= min && value <= max;
      }

      if (kind == FieldKind.Month && token.Length == 3)
      {
         value = ParseMonthName(token);
         return value >= min && value <= max;
      }

      if (kind == FieldKind.DayOfWeek && token.Length == 3)
      {
         value = ParseDayOfWeekName(token);
         return value >= min && value <= max;
      }

      value = -1;
      return false;
   }

   private static int ParseMonthName(ReadOnlySpan<char> name)
   {
      if (name.Equals("JAN", StringComparison.OrdinalIgnoreCase)) return 1;
      if (name.Equals("FEB", StringComparison.OrdinalIgnoreCase)) return 2;
      if (name.Equals("MAR", StringComparison.OrdinalIgnoreCase)) return 3;
      if (name.Equals("APR", StringComparison.OrdinalIgnoreCase)) return 4;
      if (name.Equals("MAY", StringComparison.OrdinalIgnoreCase)) return 5;
      if (name.Equals("JUN", StringComparison.OrdinalIgnoreCase)) return 6;
      if (name.Equals("JUL", StringComparison.OrdinalIgnoreCase)) return 7;
      if (name.Equals("AUG", StringComparison.OrdinalIgnoreCase)) return 8;
      if (name.Equals("SEP", StringComparison.OrdinalIgnoreCase)) return 9;
      if (name.Equals("OCT", StringComparison.OrdinalIgnoreCase)) return 10;
      if (name.Equals("NOV", StringComparison.OrdinalIgnoreCase)) return 11;
      if (name.Equals("DEC", StringComparison.OrdinalIgnoreCase)) return 12;
      return -1;
   }

   private static int ParseDayOfWeekName(ReadOnlySpan<char> name)
   {
      if (name.Equals("SUN", StringComparison.OrdinalIgnoreCase)) return 0;
      if (name.Equals("MON", StringComparison.OrdinalIgnoreCase)) return 1;
      if (name.Equals("TUE", StringComparison.OrdinalIgnoreCase)) return 2;
      if (name.Equals("WED", StringComparison.OrdinalIgnoreCase)) return 3;
      if (name.Equals("THU", StringComparison.OrdinalIgnoreCase)) return 4;
      if (name.Equals("FRI", StringComparison.OrdinalIgnoreCase)) return 5;
      if (name.Equals("SAT", StringComparison.OrdinalIgnoreCase)) return 6;
      return -1;
   }

   public bool Equals(CronSchedule other) =>
      SecondsMask == other.SecondsMask &&
      MinutesMask == other.MinutesMask &&
      HoursMask == other.HoursMask &&
      DaysOfMonthMask == other.DaysOfMonthMask &&
      MonthsMask == other.MonthsMask &&
      DaysOfWeekMask == other.DaysOfWeekMask &&
      Interval == other.Interval &&
      IsDayOfMonthWildcard == other.IsDayOfMonthWildcard &&
      IsDayOfWeekWildcard == other.IsDayOfWeekWildcard;

   public override bool Equals(object? obj) => obj is CronSchedule other && Equals(other);

   public override int GetHashCode() =>
      HashCode.Combine(SecondsMask, MinutesMask, HoursMask, DaysOfMonthMask, MonthsMask, DaysOfWeekMask, Interval);
}

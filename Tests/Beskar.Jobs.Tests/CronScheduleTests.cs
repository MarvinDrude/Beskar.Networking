using Beskar.Jobs.Common.Scheduling;

namespace Beskar.Jobs.Tests;

public class CronScheduleTests
{
   [Test]
   public async Task Parse_StandardFiveField_EvaluatesNextOccurrenceCorrectly()
   {
      // Every 15 minutes during 9am-5pm on Monday-Friday
      var schedule = CronSchedule.Parse("*/15 9-17 * * MON-FRI");

      var fridayMorning = new DateTimeOffset(2026, 10, 16, 8, 50, 0, TimeSpan.Zero); // Friday
      var first = schedule.GetNextOccurrence(fridayMorning);
      await Assert.That(first).IsEqualTo(new DateTimeOffset(2026, 10, 16, 9, 0, 0, TimeSpan.Zero));

      var second = schedule.GetNextOccurrence(first!.Value);
      await Assert.That(second).IsEqualTo(new DateTimeOffset(2026, 10, 16, 9, 15, 0, TimeSpan.Zero));

      // Friday after 17:45 should jump over Saturday (17) & Sunday (18) to Monday (19) 09:00:00
      var fridayEvening = new DateTimeOffset(2026, 10, 16, 17, 45, 0, TimeSpan.Zero);
      var mondayMorning = schedule.GetNextOccurrence(fridayEvening);
      await Assert.That(mondayMorning).IsEqualTo(new DateTimeOffset(2026, 10, 19, 9, 0, 0, TimeSpan.Zero));
   }

   [Test]
   public async Task Parse_SixFieldWithSeconds_EvaluatesNextSecondCorrectly()
   {
      var schedule = CronSchedule.Parse("10,40 * * * * *");

      var start = new DateTimeOffset(2026, 10, 17, 12, 0, 10, TimeSpan.Zero);

      // Inclusive = true should match 12:00:10
      var inclusiveMatch = schedule.GetNextOccurrence(start, inclusive: true);
      await Assert.That(inclusiveMatch).IsEqualTo(start);

      // Inclusive = false should advance to 12:00:40
      var nextMatch = schedule.GetNextOccurrence(start, inclusive: false);
      await Assert.That(nextMatch).IsEqualTo(new DateTimeOffset(2026, 10, 17, 12, 0, 40, TimeSpan.Zero));

      // Next after 12:00:40 should roll over minute to 12:01:10
      var nextMinuteMatch = schedule.GetNextOccurrence(nextMatch!.Value, inclusive: false);
      await Assert.That(nextMinuteMatch).IsEqualTo(new DateTimeOffset(2026, 10, 17, 12, 1, 10, TimeSpan.Zero));
   }

   [Test]
   public async Task Parse_StepWithRangeAndSingleStart_EvaluatesCorrectly()
   {
      // Seconds: 5/20 -> 5, 25, 45
      // Minutes: 10-30/10 -> 10, 20, 30
      var schedule = CronSchedule.Parse("5/20 10-30/10 * * * *");

      var baseTime = new DateTimeOffset(2026, 10, 17, 14, 0, 0, TimeSpan.Zero);
      var first = schedule.GetNextOccurrence(baseTime);
      await Assert.That(first).IsEqualTo(new DateTimeOffset(2026, 10, 17, 14, 10, 5, TimeSpan.Zero));

      var second = schedule.GetNextOccurrence(first!.Value);
      await Assert.That(second).IsEqualTo(new DateTimeOffset(2026, 10, 17, 14, 10, 25, TimeSpan.Zero));

      var third = schedule.GetNextOccurrence(second!.Value);
      await Assert.That(third).IsEqualTo(new DateTimeOffset(2026, 10, 17, 14, 10, 45, TimeSpan.Zero));

      var fourth = schedule.GetNextOccurrence(third!.Value);
      await Assert.That(fourth).IsEqualTo(new DateTimeOffset(2026, 10, 17, 14, 20, 5, TimeSpan.Zero));
   }

   [Test]
   public async Task Parse_SundayAsZeroSevenOrSun_AllMapToSunday()
   {
      var s0 = CronSchedule.Parse("0 0 * * 0");
      var s7 = CronSchedule.Parse("0 0 * * 7");
      var sSun = CronSchedule.Parse("0 0 * * SUN");

      await Assert.That(s0).IsEqualTo(s7);
      await Assert.That(s0).IsEqualTo(sSun);

      var saturday = new DateTimeOffset(2026, 10, 17, 12, 0, 0, TimeSpan.Zero);
      var sundayMidnight = new DateTimeOffset(2026, 10, 18, 0, 0, 0, TimeSpan.Zero);

      await Assert.That(s7.GetNextOccurrence(saturday)).IsEqualTo(sundayMidnight);
      await Assert.That(sSun.GetNextOccurrence(saturday)).IsEqualTo(sundayMidnight);
   }

   [Test]
   public async Task Parse_RestrictedDomAndDow_UsesStandardUnionSemantics()
   {
      // Both DOM (15th) and DOW (FRI) restricted -> matches either the 15th OR any Friday
      var schedule = CronSchedule.Parse("0 0 15 * FRI");

      // Oct 15, 2026 is a Thursday (matches DOM = 15)
      var oct14 = new DateTimeOffset(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);
      var next1 = schedule.GetNextOccurrence(oct14);
      await Assert.That(next1).IsEqualTo(new DateTimeOffset(2026, 10, 15, 0, 0, 0, TimeSpan.Zero));

      // Oct 16, 2026 is a Friday (matches DOW = FRI)
      var next2 = schedule.GetNextOccurrence(next1!.Value);
      await Assert.That(next2).IsEqualTo(new DateTimeOffset(2026, 10, 16, 0, 0, 0, TimeSpan.Zero));
   }

   [Test]
   public async Task Parse_ThirtyFirstOfMonth_SkipsMonthsWithThirtyDays()
   {
      var schedule = CronSchedule.Parse("0 0 31 * *");

      // Starting April 1 -> April has 30 days, so next 31st is May 31
      var april1 = new DateTimeOffset(2026, 4, 1, 0, 0, 0, TimeSpan.Zero);
      var next = schedule.GetNextOccurrence(april1);
      await Assert.That(next).IsEqualTo(new DateTimeOffset(2026, 5, 31, 0, 0, 0, TimeSpan.Zero));
   }

   [Test]
   public async Task Parse_YearRollover_AdvancesToNextYearCleanly()
   {
      var schedule = CronSchedule.Parse("0 0 1 JAN *");
      var dec31 = new DateTimeOffset(2026, 12, 31, 23, 59, 59, TimeSpan.Zero);

      var next = schedule.GetNextOccurrence(dec31);
      await Assert.That(next).IsEqualTo(new DateTimeOffset(2027, 1, 1, 0, 0, 0, TimeSpan.Zero));
   }

   [Test]
   public async Task Parse_MacrosAndIntervals_WorkAsExpected()
   {
      var baseTime = new DateTimeOffset(2026, 5, 10, 14, 23, 45, TimeSpan.Zero);

      var secondly = CronSchedule.Parse("@secondly");
      await Assert.That(secondly.GetNextOccurrence(baseTime))
         .IsEqualTo(new DateTimeOffset(2026, 5, 10, 14, 23, 46, TimeSpan.Zero));

      var minutely = CronSchedule.Parse("@minutely");
      await Assert.That(minutely.GetNextOccurrence(baseTime))
         .IsEqualTo(new DateTimeOffset(2026, 5, 10, 14, 24, 0, TimeSpan.Zero));

      var hourly = CronSchedule.Parse("@hourly");
      await Assert.That(hourly.GetNextOccurrence(baseTime))
         .IsEqualTo(new DateTimeOffset(2026, 5, 10, 15, 0, 0, TimeSpan.Zero));

      var daily = CronSchedule.Parse("@daily");
      var midnight = CronSchedule.Parse("@midnight");
      await Assert.That(daily).IsEqualTo(midnight);
      await Assert.That(daily.GetNextOccurrence(baseTime))
         .IsEqualTo(new DateTimeOffset(2026, 5, 11, 0, 0, 0, TimeSpan.Zero));

      var monthly = CronSchedule.Parse("@monthly");
      await Assert.That(monthly.GetNextOccurrence(baseTime))
         .IsEqualTo(new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero));

      var yearly = CronSchedule.Parse("@yearly");
      var annually = CronSchedule.Parse("@annually");
      await Assert.That(yearly).IsEqualTo(annually);
      await Assert.That(yearly.GetNextOccurrence(baseTime))
         .IsEqualTo(new DateTimeOffset(2027, 1, 1, 0, 0, 0, TimeSpan.Zero));

      var every250Ms = CronSchedule.Parse("@every 250ms");
      await Assert.That(every250Ms.IsInterval).IsTrue();
      await Assert.That(every250Ms.Interval).IsEqualTo(TimeSpan.FromMilliseconds(250));
      await Assert.That(every250Ms.GetNextOccurrence(baseTime))
         .IsEqualTo(baseTime.AddMilliseconds(250));

      var every2Hours = CronSchedule.Parse("@every 2h");
      await Assert.That(every2Hours.Interval).IsEqualTo(TimeSpan.FromHours(2));
      await Assert.That(every2Hours.GetNextOccurrence(baseTime))
         .IsEqualTo(baseTime.AddHours(2));
   }

   [Test]
   public async Task Parse_LeapYearFeb29_FindsNextLeapYear()
   {
      var feb29Schedule = CronSchedule.Parse("0 0 29 FEB *");
      var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

      var nextLeap = feb29Schedule.GetNextOccurrence(start);
      await Assert.That(nextLeap).IsEqualTo(new DateTimeOffset(2028, 2, 29, 0, 0, 0, TimeSpan.Zero));
   }

   [Test]
   public async Task IsMatch_VerifiesMatchingAndNonMatchingTimestamps()
   {
      var schedule = CronSchedule.Parse("0 30 10 * * MON");
      var monday1030 = new DateTimeOffset(2026, 10, 19, 10, 30, 0, TimeSpan.Zero);
      var monday1031 = new DateTimeOffset(2026, 10, 19, 10, 31, 0, TimeSpan.Zero);
      var tuesday1030 = new DateTimeOffset(2026, 10, 20, 10, 30, 0, TimeSpan.Zero);

      await Assert.That(schedule.IsMatch(monday1030)).IsTrue();
      await Assert.That(schedule.IsMatch(monday1031)).IsFalse();
      await Assert.That(schedule.IsMatch(tuesday1030)).IsFalse();
   }

   [Test]
   public async Task TryParse_InvalidExpressions_ReturnsFalse()
   {
      await Assert.That(CronSchedule.TryParse("", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("   ", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("* * *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("* * * * * * *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("60 * * * *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("* 25 * * *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("* * 0 * *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("* * * 13 *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("10-5 * * * *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("*/0 * * * *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("1, * * * *", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("@unknown", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("@every -5s", out _)).IsFalse();
      await Assert.That(CronSchedule.TryParse("@every 0ms", out _)).IsFalse();
   }
}

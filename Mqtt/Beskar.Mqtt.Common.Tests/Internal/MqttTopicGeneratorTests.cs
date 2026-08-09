using System;
using System.Text;
using Beskar.Mqtt.Common.Generators;

namespace Beskar.Mqtt.Common.Tests.Internal;

public enum SeverityEnum
{
    Low,
    Medium,
    High
}

public partial class MqttTopicGeneratorTests
{
    [GeneratedMqttTopic("devices/{deviceId}/sensors/{sensorType}")]
    public static partial bool TryParseSensorTopic(
        ReadOnlySpan<char> topic,
        out int deviceId,
        out ReadOnlySpan<char> sensorType);

    [GeneratedMqttTopic("devices/{deviceId}/sensors/{sensorType}")]
    public static partial bool IsMatchSensorTopic(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("devices/{deviceId}/sensors/{sensorType}")]
    public static partial bool IsMatchSensorTopic(ReadOnlySpan<byte> topic);

    [GeneratedMqttTopic("devices/{deviceId}/sensors/{sensorType}")]
    public static partial bool TryParseSensorTopicBytes(
        ReadOnlySpan<byte> topic,
        out int deviceId,
        out string sensorType);

    [GeneratedMqttTopic("alerts/+/critical/#")]
    public static partial bool IsCriticalAlert(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("devices/{deviceId}/sensors/{sensorType}")]
    public static partial bool TryFormatSensorTopic(
        Span<char> destination,
        int deviceId,
        ReadOnlySpan<char> sensorType,
        out int charsWritten);

    [GeneratedMqttTopic("alerts/{deviceId}/{alertId}/{isCritical}/{severity}")]
    public static partial bool TryParseAlertTopic(
        ReadOnlySpan<char> topic,
        out long deviceId,
        out Guid alertId,
        out bool isCritical,
        out SeverityEnum severity);

    [GeneratedMqttTopic("alerts/{deviceId}/{alertId}/{isCritical}/{severity}")]
    public static partial bool TryFormatAlertTopic(
        Span<char> destination,
        long deviceId,
        Guid alertId,
        bool isCritical,
        SeverityEnum severity,
        out int charsWritten);

    [GeneratedMqttTopic("devices/äöü/status/{isOk}")]
    public static partial bool TryParseNonAsciiTopic(
        ReadOnlySpan<byte> topic,
        out bool isOk);

    [GeneratedMqttTopic("devices/äöü/status")]
    public static partial bool TryFormatNonAsciiTopicBytes(
        Span<byte> destination,
        out int bytesWritten);

    [GeneratedMqttTopic("devices/\"quote\"\\slash/status/{isOk}")]
    public static partial bool TryParseEscapedTopic(
        ReadOnlySpan<char> topic,
        out bool isOk);

    [Test]
    public async Task TryParseSensorTopic_ShouldParseCorrectly()
    {
        var result = TryParseSensorTopic("devices/42/sensors/temperature", out var deviceId, out var sensorType);
        var sensorTypeStr = sensorType.ToString();
        await Assert.That(result).IsTrue();
        await Assert.That(deviceId).IsEqualTo(42);
        await Assert.That(sensorTypeStr).IsEqualTo("temperature");

        var invalidResult = TryParseSensorTopic("devices/invalid/sensors/temperature", out _, out _);
        await Assert.That(invalidResult).IsFalse();
    }

    [Test]
    public async Task TryParseSensorTopicBytes_ShouldParseCorrectly()
    {
        var bytes = "devices/99/sensors/humidity"u8;
        var result = TryParseSensorTopicBytes(bytes, out var deviceId, out var sensorType);
        await Assert.That(result).IsTrue();
        await Assert.That(deviceId).IsEqualTo(99);
        await Assert.That(sensorType).IsEqualTo("humidity");
    }

    [Test]
    public async Task IsCriticalAlert_ShouldMatchWildcards()
    {
        await Assert.That(IsCriticalAlert("alerts/temp/critical/high")).IsTrue();
        await Assert.That(IsCriticalAlert("alerts/temp/critical")).IsTrue();
        await Assert.That(IsCriticalAlert("alerts/temp/non-critical/high")).IsFalse();
        await Assert.That(IsCriticalAlert("alerts/temp")).IsFalse();
    }

    [Test]
    public async Task TryFormatSensorTopic_ShouldFormatCorrectly()
    {
        Span<char> buffer = new char[100];
        var result = TryFormatSensorTopic(buffer, 123, "pressure", out var charsWritten);
        var formatted = new string(buffer.Slice(0, charsWritten));
        await Assert.That(result).IsTrue();
        await Assert.That(formatted).IsEqualTo("devices/123/sensors/pressure");
    }

    [Test]
    public async Task GeneratedFormatterHelpers_ShouldFormatCorrectly()
    {
        var formattedString = FormatSensorTopic(123, "pressure");
        await Assert.That(formattedString).IsEqualTo("devices/123/sensors/pressure");

        var formattedBytes = FormatSensorTopicToBytes(123, "pressure");
        var decodedString = Encoding.UTF8.GetString(formattedBytes);
        await Assert.That(decodedString).IsEqualTo("devices/123/sensors/pressure");
    }

    [Test]
    public async Task TryParseAlertTopic_ShouldParseExpandedTypes()
    {
        var guidStr = "d3b07384-d113-4956-b51b-4861bc99d520";
        var topic = $"alerts/9876543210/{guidStr}/true/High";
        var result = TryParseAlertTopic(topic, out var deviceId, out var alertId, out var isCritical, out var severity);

        await Assert.That(result).IsTrue();
        await Assert.That(deviceId).IsEqualTo(9876543210L);
        await Assert.That(alertId).IsEqualTo(Guid.Parse(guidStr));
        await Assert.That(isCritical).IsTrue();
        await Assert.That(severity).IsEqualTo(SeverityEnum.High);
    }

    [Test]
    public async Task TryFormatAlertTopic_ShouldFormatExpandedTypes()
    {
        var guid = Guid.Parse("d3b07384-d113-4956-b51b-4861bc99d520");
        Span<char> buffer = new char[128];
        var result = TryFormatAlertTopic(buffer, 9876543210L, guid, true, SeverityEnum.High, out var charsWritten);
        var formatted = new string(buffer.Slice(0, charsWritten));

        await Assert.That(result).IsTrue();
        await Assert.That(formatted).IsEqualTo("alerts/9876543210/d3b07384-d113-4956-b51b-4861bc99d520/True/High");
    }

    [Test]
    public async Task GeneratedFormatterHelpers_ShouldFormatExpandedTypesCorrectly()
    {
        var guid = Guid.Parse("d3b07384-d113-4956-b51b-4861bc99d520");

        var formattedString = FormatAlertTopic(9876543210L, guid, true, SeverityEnum.High);
        await Assert.That(formattedString).IsEqualTo("alerts/9876543210/d3b07384-d113-4956-b51b-4861bc99d520/True/High");

        var formattedBytes = FormatAlertTopicToBytes(9876543210L, guid, true, SeverityEnum.High);
        var decodedString = Encoding.UTF8.GetString(formattedBytes);
        await Assert.That(decodedString).IsEqualTo("alerts/9876543210/d3b07384-d113-4956-b51b-4861bc99d520/True/High");
    }

    [Test]
    public async Task TryParseNonAsciiTopic_ShouldParseCorrectly()
    {
        var result = TryParseNonAsciiTopic("devices/äöü/status/true"u8, out var isOk);
        await Assert.That(result).IsTrue();
        await Assert.That(isOk).IsTrue();
    }

    [Test]
    public async Task TryParseEscapedTopic_ShouldParseCorrectly()
    {
        var result = TryParseEscapedTopic("devices/\"quote\"\\slash/status/true", out var isOk);
        await Assert.That(result).IsTrue();
        await Assert.That(isOk).IsTrue();
    }

    [Test]
    public async Task FormatAlertTopicToBytes_ShouldCompileWithThrowInstruction()
    {
        var method = typeof(MqttTopicGeneratorTests).GetMethod("FormatAlertTopicToBytes");
        await Assert.That(method).IsNotNull();

        var body = method!.GetMethodBody();
        await Assert.That(body).IsNotNull();

        var ilBytes = body!.GetILAsByteArray();
        await Assert.That(ilBytes).IsNotNull();

        // Check if the IL contains the throw instruction (opcode 0x7a)
        var hasThrowOpcode = false;
        foreach (var op in ilBytes!)
        {
            if (op == 0x7a) // throw opcode
            {
                hasThrowOpcode = true;
                break;
            }
        }

        await Assert.That(hasThrowOpcode).IsTrue();
    }

    [GeneratedMqttTopic("telemetry/{location}/data")]
    public static partial bool IsMatchTelemetryTopic(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("telemetry/{location}/data")]
    public static partial bool IsMatchTelemetryTopic(ReadOnlySpan<byte> topic);

    [Test]
    public async Task TryFormatNonAsciiTopicBytes_ShouldFormatCorrectly()
    {
        var buffer = new byte[100];
        var result = TryFormatNonAsciiTopicBytes(buffer, out var bytesWritten);
        await Assert.That(result).IsTrue();
        await Assert.That(bytesWritten).IsEqualTo(Encoding.UTF8.GetByteCount("devices/äöü/status"));

        var formatted = Encoding.UTF8.GetString(buffer, 0, bytesWritten);
        await Assert.That(formatted).IsEqualTo("devices/äöü/status");
    }

    [Test]
    public async Task IsMatch_WithCharSpan_ShouldMatchCorrectly()
    {
        await Assert.That(IsMatchSensorTopic("devices/42/sensors/temperature".AsSpan())).IsTrue();
        await Assert.That(IsMatchSensorTopic("devices/999/sensors/pressure".AsSpan())).IsTrue();
        await Assert.That(IsMatchSensorTopic("devices//sensors/temperature".AsSpan())).IsFalse();
        await Assert.That(IsMatchSensorTopic("devices/42/sensors/".AsSpan())).IsFalse();
        await Assert.That(IsMatchSensorTopic("invalid/42/sensors/temperature".AsSpan())).IsFalse();
        await Assert.That(IsMatchSensorTopic("".AsSpan())).IsFalse();

        await Assert.That(IsMatchTelemetryTopic("telemetry/room1/data".AsSpan())).IsTrue();
        await Assert.That(IsMatchTelemetryTopic("telemetry//data".AsSpan())).IsFalse();
    }

    [Test]
    public async Task IsMatch_WithByteSpan_ShouldMatchCorrectly()
    {
        await Assert.That(IsMatchSensorTopic("devices/42/sensors/temperature"u8)).IsTrue();
        await Assert.That(IsMatchSensorTopic("devices/999/sensors/pressure"u8)).IsTrue();
        await Assert.That(IsMatchSensorTopic("devices//sensors/temperature"u8)).IsFalse();
        await Assert.That(IsMatchSensorTopic("devices/42/sensors/"u8)).IsFalse();
        await Assert.That(IsMatchSensorTopic("invalid/42/sensors/temperature"u8)).IsFalse();
        await Assert.That(IsMatchSensorTopic(""u8)).IsFalse();

        await Assert.That(IsMatchTelemetryTopic("telemetry/room1/data"u8)).IsTrue();
        await Assert.That(IsMatchTelemetryTopic("telemetry//data"u8)).IsFalse();
    }

    [GeneratedMqttTopic("sensors/+/temperature")]
    public static partial bool IsMatchSingleWildcardMiddle(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("sensors/+/temperature")]
    public static partial bool IsMatchSingleWildcardMiddle(ReadOnlySpan<byte> topic);

    [GeneratedMqttTopic("+/status")]
    public static partial bool IsMatchSingleWildcardStart(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("+/status")]
    public static partial bool IsMatchSingleWildcardStart(ReadOnlySpan<byte> topic);

    [GeneratedMqttTopic("devices/+")]
    public static partial bool IsMatchSingleWildcardEnd(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("devices/+")]
    public static partial bool IsMatchSingleWildcardEnd(ReadOnlySpan<byte> topic);

    [GeneratedMqttTopic("finance/#")]
    public static partial bool IsMatchMultiWildcard(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("finance/#")]
    public static partial bool IsMatchMultiWildcard(ReadOnlySpan<byte> topic);

    [GeneratedMqttTopic("#")]
    public static partial bool IsMatchRootWildcard(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("#")]
    public static partial bool IsMatchRootWildcard(ReadOnlySpan<byte> topic);

    [GeneratedMqttTopic("building/+/floor/+/room/#")]
    public static partial bool IsMatchCombinedWildcards(ReadOnlySpan<char> topic);

    [GeneratedMqttTopic("building/+/floor/+/room/#")]
    public static partial bool IsMatchCombinedWildcards(ReadOnlySpan<byte> topic);

    [Test]
    public async Task IsMatch_WildcardsCharSpan_ShouldMatchCorrectly()
    {
        // Single wildcard + in middle
        await Assert.That(IsMatchSingleWildcardMiddle("sensors/room1/temperature".AsSpan())).IsTrue();
        await Assert.That(IsMatchSingleWildcardMiddle("sensors/building2/temperature".AsSpan())).IsTrue();
        await Assert.That(IsMatchSingleWildcardMiddle("sensors//temperature".AsSpan())).IsFalse();
        await Assert.That(IsMatchSingleWildcardMiddle("sensors/room1/sub/temperature".AsSpan())).IsFalse();
        await Assert.That(IsMatchSingleWildcardMiddle("sensors/temperature".AsSpan())).IsFalse();

        // Single wildcard + at start
        await Assert.That(IsMatchSingleWildcardStart("device1/status".AsSpan())).IsTrue();
        await Assert.That(IsMatchSingleWildcardStart("device2/status".AsSpan())).IsTrue();
        await Assert.That(IsMatchSingleWildcardStart("device1/sub/status".AsSpan())).IsFalse();
        await Assert.That(IsMatchSingleWildcardStart("status".AsSpan())).IsFalse();

        // Single wildcard + at end
        await Assert.That(IsMatchSingleWildcardEnd("devices/1".AsSpan())).IsTrue();
        await Assert.That(IsMatchSingleWildcardEnd("devices/abc".AsSpan())).IsTrue();
        await Assert.That(IsMatchSingleWildcardEnd("devices/1/2".AsSpan())).IsFalse();
        await Assert.That(IsMatchSingleWildcardEnd("devices/".AsSpan())).IsFalse();

        // Multi wildcard #
        await Assert.That(IsMatchMultiWildcard("finance".AsSpan())).IsTrue();
        await Assert.That(IsMatchMultiWildcard("finance/".AsSpan())).IsTrue();
        await Assert.That(IsMatchMultiWildcard("finance/stocks".AsSpan())).IsTrue();
        await Assert.That(IsMatchMultiWildcard("finance/stocks/nasdaq/aapl".AsSpan())).IsTrue();
        await Assert.That(IsMatchMultiWildcard("financial/stocks".AsSpan())).IsFalse();

        // Root wildcard #
        await Assert.That(IsMatchRootWildcard("anything".AsSpan())).IsTrue();
        await Assert.That(IsMatchRootWildcard("a/b/c/d".AsSpan())).IsTrue();

        // Combined wildcards + and #
        await Assert.That(IsMatchCombinedWildcards("building/A/floor/3/room/101".AsSpan())).IsTrue();
        await Assert.That(IsMatchCombinedWildcards("building/A/floor/3/room/101/sensor/temp".AsSpan())).IsTrue();
        await Assert.That(IsMatchCombinedWildcards("building/A/floor/3/room".AsSpan())).IsTrue();
        await Assert.That(IsMatchCombinedWildcards("building/A/floor/room/101".AsSpan())).IsFalse();
    }

    [Test]
    public async Task IsMatch_WildcardsByteSpan_ShouldMatchCorrectly()
    {
        // Single wildcard + in middle
        await Assert.That(IsMatchSingleWildcardMiddle("sensors/room1/temperature"u8)).IsTrue();
        await Assert.That(IsMatchSingleWildcardMiddle("sensors/building2/temperature"u8)).IsTrue();
        await Assert.That(IsMatchSingleWildcardMiddle("sensors//temperature"u8)).IsFalse();
        await Assert.That(IsMatchSingleWildcardMiddle("sensors/room1/sub/temperature"u8)).IsFalse();
        await Assert.That(IsMatchSingleWildcardMiddle("sensors/temperature"u8)).IsFalse();

        // Single wildcard + at start
        await Assert.That(IsMatchSingleWildcardStart("device1/status"u8)).IsTrue();
        await Assert.That(IsMatchSingleWildcardStart("device2/status"u8)).IsTrue();
        await Assert.That(IsMatchSingleWildcardStart("device1/sub/status"u8)).IsFalse();
        await Assert.That(IsMatchSingleWildcardStart("status"u8)).IsFalse();

        // Single wildcard + at end
        await Assert.That(IsMatchSingleWildcardEnd("devices/1"u8)).IsTrue();
        await Assert.That(IsMatchSingleWildcardEnd("devices/abc"u8)).IsTrue();
        await Assert.That(IsMatchSingleWildcardEnd("devices/1/2"u8)).IsFalse();
        await Assert.That(IsMatchSingleWildcardEnd("devices/"u8)).IsFalse();

        // Multi wildcard #
        await Assert.That(IsMatchMultiWildcard("finance"u8)).IsTrue();
        await Assert.That(IsMatchMultiWildcard("finance/"u8)).IsTrue();
        await Assert.That(IsMatchMultiWildcard("finance/stocks"u8)).IsTrue();
        await Assert.That(IsMatchMultiWildcard("finance/stocks/nasdaq/aapl"u8)).IsTrue();
        await Assert.That(IsMatchMultiWildcard("financial/stocks"u8)).IsFalse();

        // Root wildcard #
        await Assert.That(IsMatchRootWildcard("anything"u8)).IsTrue();
        await Assert.That(IsMatchRootWildcard("a/b/c/d"u8)).IsTrue();

        // Combined wildcards + and #
        await Assert.That(IsMatchCombinedWildcards("building/A/floor/3/room/101"u8)).IsTrue();
        await Assert.That(IsMatchCombinedWildcards("building/A/floor/3/room/101/sensor/temp"u8)).IsTrue();
        await Assert.That(IsMatchCombinedWildcards("building/A/floor/3/room"u8)).IsTrue();
        await Assert.That(IsMatchCombinedWildcards("building/A/floor/room/101"u8)).IsFalse();
    }
}

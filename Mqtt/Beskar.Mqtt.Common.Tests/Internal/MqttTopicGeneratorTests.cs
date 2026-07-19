using System;
using System.Text;
using Beskar.Mqtt.Common.Generators;

namespace Beskar.Mqtt.Common.Tests.Internal;

public partial class MqttTopicGeneratorTests
{
    [GeneratedMqttTopic("devices/{deviceId}/sensors/{sensorType}")]
    public static partial bool TryParseSensorTopic(
        ReadOnlySpan<char> topic,
        out int deviceId,
        out ReadOnlySpan<char> sensorType);

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
        var formatted = new string(buffer[..charsWritten]);
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
}

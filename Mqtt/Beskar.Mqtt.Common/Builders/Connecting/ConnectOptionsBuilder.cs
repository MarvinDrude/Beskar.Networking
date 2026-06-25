using System;
using System.Buffers;
using System.Text;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Connecting;

public sealed class ConnectOptionsBuilder(ConnectOptions? options = null)
   : UserPropertiesBaseOptionsBuilder<ConnectOptionsBuilder, ConnectOptions>(options ?? new ConnectOptions())
{
   public ConnectOptionsBuilder WithProtocolVersion(MqttProtocolVersion version)
   {
      _options.ProtocolVersion = version;
      return this;
   }

   public ConnectOptionsBuilder WithCleanSession(bool cleanSession = true)
   {
      _options.CleanSession = cleanSession;
      return this;
   }

   public ConnectOptionsBuilder WithKeepAlivePeriod(ushort keepAlivePeriod)
   {
      _options.KeepAlivePeriod = keepAlivePeriod;
      return this;
   }

   public ConnectOptionsBuilder WithTimeout(TimeSpan timeout)
   {
      _options.Timeout = timeout;
      return this;
   }

   public ConnectOptionsBuilder WithClientId(string clientId)
   {
      _options.ClientIdUtf8Bytes = Encoding.UTF8.GetBytes(clientId);
      return this;
   }

   public ConnectOptionsBuilder WithClientId(ReadOnlySpan<byte> clientIdUtf8Bytes)
   {
      _options.ClientIdUtf8Bytes = clientIdUtf8Bytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithClientId(ReadOnlyMemory<byte> clientIdUtf8Bytes)
   {
      _options.ClientIdUtf8Bytes = clientIdUtf8Bytes;
      return this;
   }

   public ConnectOptionsBuilder WithUsername(string username)
   {
      _options.UsernameUtf8Bytes = Encoding.UTF8.GetBytes(username);
      return this;
   }

   public ConnectOptionsBuilder WithUsername(ReadOnlySpan<char> username)
   {
      _options.UsernameUtf8Bytes = Encoding.UTF8.GetBytes([.. username]);
      return this;
   }

   public ConnectOptionsBuilder WithUsername(ReadOnlySpan<byte> usernameUtf8Bytes)
   {
      _options.UsernameUtf8Bytes = usernameUtf8Bytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithUsername(ReadOnlyMemory<byte> usernameUtf8Bytes)
   {
      _options.UsernameUtf8Bytes = usernameUtf8Bytes;
      return this;
   }

   public ConnectOptionsBuilder WithPassword(byte[] password)
   {
      _options.PasswordBytes = password;
      return this;
   }

   public ConnectOptionsBuilder WithPassword(ReadOnlySpan<byte> passwordBytes)
   {
      _options.PasswordBytes = passwordBytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithPassword(ReadOnlyMemory<byte> passwordBytes)
   {
      _options.PasswordBytes = passwordBytes;
      return this;
   }

   public ConnectOptionsBuilder WithPassword(string password)
   {
      _options.PasswordBytes = Encoding.UTF8.GetBytes(password);
      return this;
   }

   public ConnectOptionsBuilder WithPassword(ReadOnlySpan<char> password)
   {
      _options.PasswordBytes = Encoding.UTF8.GetBytes([.. password]);
      return this;
   }

   public ConnectOptionsBuilder WithSessionExpiryInterval(uint interval)
   {
      _options.SessionExpiryInterval = interval;
      return this;
   }

   public ConnectOptionsBuilder WithTopicAliasMaximum(ushort maximum)
   {
      _options.TopicAliasMaximum = maximum;
      return this;
   }

   public ConnectOptionsBuilder WithMaximumPacketSize(uint size)
   {
      _options.MaximumPacketSize = size;
      return this;
   }

   public ConnectOptionsBuilder WithRequestResponseInformation(bool request = true)
   {
      _options.RequestResponseInformation = request;
      return this;
   }

   public ConnectOptionsBuilder WithRequestProblemInformation(bool request = true)
   {
      _options.RequestProblemInformation = request;
      return this;
   }

   public ConnectOptionsBuilder WithAuthenticationMethod(string method)
   {
      _options.AuthenticationMethodUtf8Bytes = Encoding.UTF8.GetBytes(method);
      return this;
   }

   public ConnectOptionsBuilder WithAuthenticationMethod(ReadOnlySpan<char> method)
   {
      _options.AuthenticationMethodUtf8Bytes = Encoding.UTF8.GetBytes([.. method]);
      return this;
   }

   public ConnectOptionsBuilder WithAuthenticationMethod(ReadOnlySpan<byte> methodUtf8Bytes)
   {
      _options.AuthenticationMethodUtf8Bytes = methodUtf8Bytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithAuthenticationMethod(ReadOnlyMemory<byte> methodUtf8Bytes)
   {
      _options.AuthenticationMethodUtf8Bytes = methodUtf8Bytes;
      return this;
   }

   public ConnectOptionsBuilder WithAuthenticationData(byte[] data)
   {
      _options.AuthenticationDataBytes = data;
      return this;
   }

   public ConnectOptionsBuilder WithAuthenticationData(ReadOnlySpan<byte> dataBytes)
   {
      _options.AuthenticationDataBytes = dataBytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithAuthenticationData(ReadOnlyMemory<byte> dataBytes)
   {
      _options.AuthenticationDataBytes = dataBytes;
      return this;
   }

   public ConnectOptionsBuilder WithTryPrivate(bool tryPrivate = true)
   {
      _options.TryPrivate = tryPrivate;
      return this;
   }

   // Will options
   public ConnectOptionsBuilder WithWill(
      string topic,
      byte[] payload,
      QualityOfServiceType qos = QualityOfServiceType.AtMostOnce,
      bool retain = false)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = Encoding.UTF8.GetBytes(topic);
      _options.WillPayload = new ReadOnlySequence<byte>(payload);
      _options.WillQualityOfService = qos;
      _options.WillRetain = retain;
      return this;
   }


   public ConnectOptionsBuilder WithWillTopic(string topic)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = Encoding.UTF8.GetBytes(topic);
      return this;
   }

   public ConnectOptionsBuilder WithWillTopic(ReadOnlySpan<char> topic)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = Encoding.UTF8.GetBytes([.. topic]);
      return this;
   }

   public ConnectOptionsBuilder WithWillTopic(ReadOnlySpan<byte> topicUtf8Bytes)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = topicUtf8Bytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithWillTopic(ReadOnlyMemory<byte> topicUtf8Bytes)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = topicUtf8Bytes;
      return this;
   }

   public ConnectOptionsBuilder WithWillPayload(byte[] payload)
   {
      _options.WillPayload = new ReadOnlySequence<byte>(payload);
      return this;
   }

   public ConnectOptionsBuilder WithWillPayload(ReadOnlySpan<byte> payload)
   {
      _options.WillPayload = new ReadOnlySequence<byte>(payload.ToArray());
      return this;
   }

   public ConnectOptionsBuilder WithWillPayload(ReadOnlySequence<byte> payload)
   {
      _options.WillPayload = payload;
      return this;
   }

   public ConnectOptionsBuilder WithWillPayload(ReadOnlyMemory<byte> payload)
   {
      _options.WillPayload = new ReadOnlySequence<byte>(payload);
      return this;
   }

   public ConnectOptionsBuilder WithWillPayload(string payload)
   {
      _options.WillPayload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(payload));
      return this;
   }

   public ConnectOptionsBuilder WithWillQualityOfService(QualityOfServiceType qos)
   {
      _options.WillQualityOfService = qos;
      return this;
   }

   public ConnectOptionsBuilder WithWillRetain(bool retain = true)
   {
      _options.WillRetain = retain;
      return this;
   }

   public ConnectOptionsBuilder WithWillDelayInterval(uint interval)
   {
      _options.WillDelayInterval = interval;
      return this;
   }

   public ConnectOptionsBuilder WithWillPayloadFormat(PayloadFormat format)
   {
      _options.WillPayloadFormatIndicator = format;
      return this;
   }

   public ConnectOptionsBuilder WithWillMessageExpiryInterval(uint interval)
   {
      _options.WillMessageExpiryInterval = interval;
      return this;
   }

   public ConnectOptionsBuilder WithWillContentType(string contentType)
   {
      _options.WillContentTypeUtf8Bytes = Encoding.UTF8.GetBytes(contentType);
      return this;
   }

   public ConnectOptionsBuilder WithWillContentType(ReadOnlySpan<char> contentType)
   {
      _options.WillContentTypeUtf8Bytes = Encoding.UTF8.GetBytes([.. contentType]);
      return this;
   }

   public ConnectOptionsBuilder WithWillContentType(ReadOnlySpan<byte> contentTypeUtf8Bytes)
   {
      _options.WillContentTypeUtf8Bytes = contentTypeUtf8Bytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithWillContentType(ReadOnlyMemory<byte> contentTypeUtf8Bytes)
   {
      _options.WillContentTypeUtf8Bytes = contentTypeUtf8Bytes;
      return this;
   }

   public ConnectOptionsBuilder WithWillResponseTopic(string responseTopic)
   {
      _options.WillResponseTopicUtf8Bytes = Encoding.UTF8.GetBytes(responseTopic);
      return this;
   }

   public ConnectOptionsBuilder WithWillResponseTopic(ReadOnlySpan<char> responseTopic)
   {
      _options.WillResponseTopicUtf8Bytes = Encoding.UTF8.GetBytes([.. responseTopic]);
      return this;
   }

   public ConnectOptionsBuilder WithWillResponseTopic(ReadOnlySpan<byte> responseTopicUtf8Bytes)
   {
      _options.WillResponseTopicUtf8Bytes = responseTopicUtf8Bytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithWillResponseTopic(ReadOnlyMemory<byte> responseTopicUtf8Bytes)
   {
      _options.WillResponseTopicUtf8Bytes = responseTopicUtf8Bytes;
      return this;
   }

   public ConnectOptionsBuilder WithWillCorrelationData(byte[] correlationData)
   {
      _options.WillCorrelationDataBytes = correlationData;
      return this;
   }

   public ConnectOptionsBuilder WithWillCorrelationData(ReadOnlySpan<byte> correlationDataBytes)
   {
      _options.WillCorrelationDataBytes = correlationDataBytes.ToArray();
      return this;
   }

   public ConnectOptionsBuilder WithWillCorrelationData(ReadOnlyMemory<byte> correlationDataBytes)
   {
      _options.WillCorrelationDataBytes = correlationDataBytes;
      return this;
   }

   public ConnectOptionsBuilder WithWillUserProperty(string name, string value)
   {
      _options.WillUserProperties.Add(name, value);
      return this;
   }

   public ConnectOptionsBuilder WithWillUserProperty(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
   {
      _options.WillUserProperties.Add(name, value);
      return this;
   }

   public ConnectOptionsBuilder WithWillUserProperty(ReadOnlySpan<byte> nameUtf8Bytes, ReadOnlySpan<byte> valueUtf8Bytes)
   {
      _options.WillUserProperties.Add(nameUtf8Bytes, valueUtf8Bytes);
      return this;
   }
}

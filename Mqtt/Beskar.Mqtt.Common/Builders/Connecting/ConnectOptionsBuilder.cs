using System;
using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Connecting;

/// <summary>
/// A fluent builder for creating <see cref="ConnectOptions"/>.
/// </summary>
public sealed class ConnectOptionsBuilder(IPEndPoint endPoint, ConnectOptions? options = null)
   : UserPropertiesBaseOptionsBuilder<ConnectOptionsBuilder, ConnectOptions>(options ?? new ConnectOptions { EndPoint = endPoint })
{
   /// <summary>
   /// Sets the MQTT protocol version to use for the connection.
   /// </summary>
   public ConnectOptionsBuilder WithProtocolVersion(MqttProtocolVersion version)
   {
      _options.ProtocolVersion = version;
      return this;
   }

   /// <summary>
   /// Sets whether to start a clean session/start.
   /// </summary>
   public ConnectOptionsBuilder WithCleanSession(bool cleanSession = true)
   {
      _options.CleanSession = cleanSession;
      return this;
   }

   /// <summary>
   /// Sets the keep alive period in seconds.
   /// </summary>
   public ConnectOptionsBuilder WithKeepAlivePeriod(ushort keepAlivePeriod)
   {
      _options.KeepAlivePeriod = keepAlivePeriod;
      return this;
   }

   /// <summary>
   /// Sets the timeout for the network connection attempt.
   /// </summary>
   public ConnectOptionsBuilder WithTimeout(TimeSpan timeout)
   {
      _options.Timeout = timeout;
      return this;
   }

   /// <summary>
   /// Sets the Client Identifier.
   /// </summary>
   public ConnectOptionsBuilder WithClientId(string clientId)
   {
      _options.ClientIdUtf8Bytes = Encoding.UTF8.GetBytes(clientId);
      return this;
   }

   /// <summary>
   /// Sets the Client Identifier.
   /// </summary>
   public ConnectOptionsBuilder WithClientId(ReadOnlySpan<char> clientId)
   {
      _options.ClientIdUtf8Bytes = Encoding.UTF8.GetBytes([.. clientId]);
      return this;
   }

   /// <summary>
   /// Sets the Client Identifier.
   /// </summary>
   public ConnectOptionsBuilder WithClientId(ReadOnlySpan<byte> clientIdUtf8Bytes)
   {
      _options.ClientIdUtf8Bytes = clientIdUtf8Bytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Client Identifier.
   /// </summary>
   public ConnectOptionsBuilder WithClientId(ReadOnlyMemory<byte> clientIdUtf8Bytes)
   {
      _options.ClientIdUtf8Bytes = clientIdUtf8Bytes;
      return this;
   }

   /// <summary>
   /// Sets the Username.
   /// </summary>
   public ConnectOptionsBuilder WithUsername(string username)
   {
      _options.UsernameUtf8Bytes = Encoding.UTF8.GetBytes(username);
      return this;
   }

   /// <summary>
   /// Sets the Username.
   /// </summary>
   public ConnectOptionsBuilder WithUsername(ReadOnlySpan<char> username)
   {
      _options.UsernameUtf8Bytes = Encoding.UTF8.GetBytes([.. username]);
      return this;
   }

   /// <summary>
   /// Sets the Username.
   /// </summary>
   public ConnectOptionsBuilder WithUsername(ReadOnlySpan<byte> usernameUtf8Bytes)
   {
      _options.UsernameUtf8Bytes = usernameUtf8Bytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Username.
   /// </summary>
   public ConnectOptionsBuilder WithUsername(ReadOnlyMemory<byte> usernameUtf8Bytes)
   {
      _options.UsernameUtf8Bytes = usernameUtf8Bytes;
      return this;
   }

   /// <summary>
   /// Sets the Password.
   /// </summary>
   public ConnectOptionsBuilder WithPassword(byte[] password)
   {
      _options.PasswordBytes = password;
      return this;
   }

   /// <summary>
   /// Sets the Password.
   /// </summary>
   public ConnectOptionsBuilder WithPassword(ReadOnlySpan<byte> passwordBytes)
   {
      _options.PasswordBytes = passwordBytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Password.
   /// </summary>
   public ConnectOptionsBuilder WithPassword(ReadOnlyMemory<byte> passwordBytes)
   {
      _options.PasswordBytes = passwordBytes;
      return this;
   }

   /// <summary>
   /// Sets the Password.
   /// </summary>
   public ConnectOptionsBuilder WithPassword(string password)
   {
      _options.PasswordBytes = Encoding.UTF8.GetBytes(password);
      return this;
   }

   /// <summary>
   /// Sets the Password.
   /// </summary>
   public ConnectOptionsBuilder WithPassword(ReadOnlySpan<char> password)
   {
      _options.PasswordBytes = Encoding.UTF8.GetBytes([.. password]);
      return this;
   }

   /// <summary>
   /// Sets the Session Expiry Interval in seconds.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithSessionExpiryInterval(uint interval)
   {
      _options.SessionExpiryInterval = interval;
      return this;
   }

   /// <summary>
   /// Sets the Topic Alias Maximum.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithTopicAliasMaximum(ushort maximum)
   {
      _options.TopicAliasMaximum = maximum;
      return this;
   }

   /// <summary>
   /// Sets the Maximum Packet Size.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithMaximumPacketSize(uint size)
   {
      _options.MaximumPacketSize = size;
      return this;
   }

   /// <summary>
   /// Sets the Request Response Information.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithRequestResponseInformation(bool request = true)
   {
      _options.RequestResponseInformation = request;
      return this;
   }

   /// <summary>
   /// Sets the Request Problem Information.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithRequestProblemInformation(bool request = true)
   {
      _options.RequestProblemInformation = request;
      return this;
   }

   /// <summary>
   /// Sets the Authentication Method.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithAuthenticationMethod(string method)
   {
      _options.AuthenticationMethodUtf8Bytes = Encoding.UTF8.GetBytes(method);
      return this;
   }

   /// <summary>
   /// Sets the Authentication Method.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithAuthenticationMethod(ReadOnlySpan<char> method)
   {
      _options.AuthenticationMethodUtf8Bytes = Encoding.UTF8.GetBytes([.. method]);
      return this;
   }

   /// <summary>
   /// Sets the Authentication Method.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithAuthenticationMethod(ReadOnlySpan<byte> methodUtf8Bytes)
   {
      _options.AuthenticationMethodUtf8Bytes = methodUtf8Bytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Authentication Method.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithAuthenticationMethod(ReadOnlyMemory<byte> methodUtf8Bytes)
   {
      _options.AuthenticationMethodUtf8Bytes = methodUtf8Bytes;
      return this;
   }

   /// <summary>
   /// Sets the Authentication Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithAuthenticationData(byte[] data)
   {
      _options.AuthenticationDataBytes = data;
      return this;
   }

   /// <summary>
   /// Sets the Authentication Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithAuthenticationData(ReadOnlySpan<byte> dataBytes)
   {
      _options.AuthenticationDataBytes = dataBytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Authentication Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithAuthenticationData(ReadOnlyMemory<byte> dataBytes)
   {
      _options.AuthenticationDataBytes = dataBytes;
      return this;
   }

   /// <summary>
   /// Tries to establish a private bridge connection (Mosquitto/MQTT 3.1.1 Bridge flag).
   /// </summary>
   public ConnectOptionsBuilder WithTryPrivate(bool tryPrivate = true)
   {
      _options.TryPrivate = tryPrivate;
      return this;
   }

   /// <summary>
   /// Configures the Will Topic, Will Payload, Will Quality of Service, and Will Retain.
   /// </summary>
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

   /// <summary>
   /// Sets the Will Topic.
   /// </summary>
   public ConnectOptionsBuilder WithWillTopic(string topic)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = Encoding.UTF8.GetBytes(topic);
      return this;
   }

   /// <summary>
   /// Sets the Will Topic.
   /// </summary>
   public ConnectOptionsBuilder WithWillTopic(ReadOnlySpan<char> topic)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = Encoding.UTF8.GetBytes([.. topic]);
      return this;
   }

   /// <summary>
   /// Sets the Will Topic.
   /// </summary>
   public ConnectOptionsBuilder WithWillTopic(ReadOnlySpan<byte> topicUtf8Bytes)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = topicUtf8Bytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Will Topic.
   /// </summary>
   public ConnectOptionsBuilder WithWillTopic(ReadOnlyMemory<byte> topicUtf8Bytes)
   {
      _options.HasWill = true;
      _options.WillTopicUtf8Bytes = topicUtf8Bytes;
      return this;
   }

   /// <summary>
   /// Sets the Will Payload.
   /// </summary>
   public ConnectOptionsBuilder WithWillPayload(byte[] payload)
   {
      _options.WillPayload = new ReadOnlySequence<byte>(payload);
      return this;
   }

   /// <summary>
   /// Sets the Will Payload.
   /// </summary>
   public ConnectOptionsBuilder WithWillPayload(ReadOnlySpan<byte> payload)
   {
      _options.WillPayload = new ReadOnlySequence<byte>(payload.ToArray());
      return this;
   }

   /// <summary>
   /// Sets the Will Payload.
   /// </summary>
   public ConnectOptionsBuilder WithWillPayload(ReadOnlySequence<byte> payload)
   {
      _options.WillPayload = payload;
      return this;
   }

   /// <summary>
   /// Sets the Will Payload.
   /// </summary>
   public ConnectOptionsBuilder WithWillPayload(ReadOnlyMemory<byte> payload)
   {
      _options.WillPayload = new ReadOnlySequence<byte>(payload);
      return this;
   }

   /// <summary>
   /// Sets the Will Payload.
   /// </summary>
   public ConnectOptionsBuilder WithWillPayload(string payload)
   {
      _options.WillPayload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(payload));
      return this;
   }

   /// <summary>
   /// Sets the Will Quality of Service.
   /// </summary>
   public ConnectOptionsBuilder WithWillQualityOfService(QualityOfServiceType qos)
   {
      _options.WillQualityOfService = qos;
      return this;
   }

   /// <summary>
   /// Sets whether the Will Message should be retained.
   /// </summary>
   public ConnectOptionsBuilder WithWillRetain(bool retain = true)
   {
      _options.WillRetain = retain;
      return this;
   }

   /// <summary>
   /// Sets the Will Delay Interval.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillDelayInterval(uint interval)
   {
      _options.WillDelayInterval = interval;
      return this;
   }

   /// <summary>
   /// Sets the Will Payload Format Indicator.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillPayloadFormat(PayloadFormat format)
   {
      _options.WillPayloadFormatIndicator = format;
      return this;
   }

   /// <summary>
   /// Sets the Will Message Expiry Interval.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillMessageExpiryInterval(uint interval)
   {
      _options.WillMessageExpiryInterval = interval;
      return this;
   }

   /// <summary>
   /// Sets the Will Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillContentType(string contentType)
   {
      _options.WillContentTypeUtf8Bytes = Encoding.UTF8.GetBytes(contentType);
      return this;
   }

   /// <summary>
   /// Sets the Will Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillContentType(ReadOnlySpan<char> contentType)
   {
      _options.WillContentTypeUtf8Bytes = Encoding.UTF8.GetBytes([.. contentType]);
      return this;
   }

   /// <summary>
   /// Sets the Will Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillContentType(ReadOnlySpan<byte> contentTypeUtf8Bytes)
   {
      _options.WillContentTypeUtf8Bytes = contentTypeUtf8Bytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Will Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillContentType(ReadOnlyMemory<byte> contentTypeUtf8Bytes)
   {
      _options.WillContentTypeUtf8Bytes = contentTypeUtf8Bytes;
      return this;
   }

   /// <summary>
   /// Sets the Will Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillResponseTopic(string responseTopic)
   {
      _options.WillResponseTopicUtf8Bytes = Encoding.UTF8.GetBytes(responseTopic);
      return this;
   }

   /// <summary>
   /// Sets the Will Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillResponseTopic(ReadOnlySpan<char> responseTopic)
   {
      _options.WillResponseTopicUtf8Bytes = Encoding.UTF8.GetBytes([.. responseTopic]);
      return this;
   }

   /// <summary>
   /// Sets the Will Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillResponseTopic(ReadOnlySpan<byte> responseTopicUtf8Bytes)
   {
      _options.WillResponseTopicUtf8Bytes = responseTopicUtf8Bytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Will Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillResponseTopic(ReadOnlyMemory<byte> responseTopicUtf8Bytes)
   {
      _options.WillResponseTopicUtf8Bytes = responseTopicUtf8Bytes;
      return this;
   }

   /// <summary>
   /// Sets the Will Correlation Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillCorrelationData(byte[] correlationData)
   {
      _options.WillCorrelationDataBytes = correlationData;
      return this;
   }

   /// <summary>
   /// Sets the Will Correlation Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillCorrelationData(ReadOnlySpan<byte> correlationDataBytes)
   {
      _options.WillCorrelationDataBytes = correlationDataBytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Will Correlation Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillCorrelationData(ReadOnlyMemory<byte> correlationDataBytes)
   {
      _options.WillCorrelationDataBytes = correlationDataBytes;
      return this;
   }

   /// <summary>
   /// Appends a new User Property to the Will.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillUserProperty(string name, string value)
   {
      _options.WillUserProperties.Add(name, value);
      return this;
   }

   /// <summary>
   /// Appends a new User Property to the Will.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillUserProperty(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
   {
      _options.WillUserProperties.Add(name, value);
      return this;
   }

   /// <summary>
   /// Appends a new User Property to the Will.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectOptionsBuilder WithWillUserProperty(ReadOnlySpan<byte> nameUtf8Bytes, ReadOnlySpan<byte> valueUtf8Bytes)
   {
      _options.WillUserProperties.Add(nameUtf8Bytes, valueUtf8Bytes);
      return this;
   }

   /// <summary>
   /// Sets the provider used to set Username and Password
   /// in the CONNECT packet.
   /// </summary>
   public ConnectOptionsBuilder WithCredentialsProvider(IMqttCredentialsProvider credentialsProvider)
   {
      _options.CredentialsProvider = credentialsProvider;
      return this;
   }

   /// <summary>
   /// Used to control how the auth flow should interact with the
   /// auth challenge from the server.
   /// </summary>
   public ConnectOptionsBuilder WithAuthHandler(IMqttAuthenticationHandler authHandler)
   {
      _options.AuthenticationHandler = authHandler;
      return this;
   }
}

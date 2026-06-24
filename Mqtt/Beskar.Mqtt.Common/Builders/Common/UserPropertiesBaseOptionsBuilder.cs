using System.Runtime.CompilerServices;

namespace Beskar.Mqtt.Common.Builders.Common;

public abstract class UserPropertiesBaseOptionsBuilder<TSelf, TOptions>(TOptions options)
   where TOptions : UserPropertiesBaseOptions
   where TSelf : UserPropertiesBaseOptionsBuilder<TSelf, TOptions>
{
   protected readonly TOptions _options = options;

   /// <summary>
   /// Builds the options
   /// </summary>
   public TOptions Build() => _options;

   /// <summary>
   /// Appends a new user property to the options.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   /// <param name="nameUtf8Bytes">Name string as utf8 bytes (you can use u8 if it's a constant)</param>
   /// <param name="valueUtf8Bytes">Value string as utf8 bytes (you can use u8 if it's a constant)</param>
   public TSelf WithUserProperty(ReadOnlySpan<byte> nameUtf8Bytes, ReadOnlySpan<byte> valueUtf8Bytes)
   {
      _options.UserProperties.Add(nameUtf8Bytes, valueUtf8Bytes);
      return Reinterpret();
   }

   /// <summary>
   /// Appends a new user property to the options.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   /// <param name="name">Name string as char span</param>
   /// <param name="value">Value string as char span</param>
   public TSelf WithUserProperty(ReadOnlySpan<char> name, ReadOnlySpan<char> value)
   {
      _options.UserProperties.Add(name, value);
      return Reinterpret();
   }

   /// <summary>
   /// Appends a new user property to the options.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   /// <param name="name">Name string as char span</param>
   /// <param name="valueBytes">Value as byte span</param>
   public TSelf WithUserProperty(ReadOnlySpan<char> name, ReadOnlySpan<byte> valueBytes)
   {
      _options.UserProperties.Add(name, valueBytes);
      return Reinterpret();
   }

   /// <summary>
   /// Appends a new user property to the options.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   /// <param name="name">Name string</param>
   /// <param name="valueBytes">Value as byte span</param>
   public TSelf WithUserProperty(string name, ReadOnlySpan<byte> valueBytes)
   {
      _options.UserProperties.Add(name, valueBytes);
      return Reinterpret();
   }

   /// <summary>
   /// Appends a new user property to the options. (there are better overloads)
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   /// <param name="name">Name string</param>
   /// <param name="value">Value string</param>
   public TSelf WithUserProperty(string name, string value)
   {
      _options.UserProperties.Add(name, value);
      return Reinterpret();
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   private TSelf Reinterpret() => Unsafe.As<TSelf>(this);
}

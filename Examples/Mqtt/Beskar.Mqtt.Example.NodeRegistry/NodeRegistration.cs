using System.Text.Json;

namespace Beskar.Mqtt.Example.NodeRegistry;

/// <summary>
/// Metadata announced by each node in the MQTT node registry.
/// </summary>
public sealed record NodeRegistration(
   string NodeId,
   string Role,
   string Endpoint,
   string Version,
   string[] Capabilities,
   DateTimeOffset StartedAtUtc,
   DateTimeOffset LastUpdatedAtUtc,
   int ActiveLoadPercent = 0)
{
   public static readonly JsonSerializerOptions JsonOptions = new()
   {
      PropertyNamingPolicy = JsonNamingPolicy.CamelCase
   };
}

/// <summary>
/// Topic conventions used by the MQTT node registry.
/// </summary>
public static class NodeRegistryTopics
{
   public const string Prefix = "registry/nodes/";
   public const string AllNodesFilter = "registry/nodes/+";

   public static string ForNode(string nodeId)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
      return $"{Prefix}{nodeId}";
   }

   public static bool TryExtractNodeId(string topic, out string nodeId)
   {
      if (topic.StartsWith(Prefix, StringComparison.Ordinal) && topic.Length > Prefix.Length)
      {
         nodeId = topic[Prefix.Length..];
         return !nodeId.Contains('/');
      }

      nodeId = string.Empty;
      return false;
   }
}

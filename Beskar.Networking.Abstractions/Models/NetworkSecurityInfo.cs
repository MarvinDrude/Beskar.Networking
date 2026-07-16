using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// Represents security information for a network session.
/// </summary>
public readonly record struct NetworkSecurityInfo(
   bool IsEncrypted,
   SslProtocols? Protocol = null,
   string? CipherSuite = null,
   X509Certificate? LocalCertificate = null,
   X509Certificate? RemoteCertificate = null);

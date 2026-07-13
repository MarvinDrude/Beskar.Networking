using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Beskar.Networking.Benchmarks.Common;

public static class CertificateHelper
{
   public static X509Certificate2 GenerateSelfSignedCertificate()
   {
      using var rsa = RSA.Create(2048);
      var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

      var sanBuilder = new SubjectAlternativeNameBuilder();
      sanBuilder.AddDnsName("localhost");
      sanBuilder.AddIpAddress(IPAddress.Loopback);
      sanBuilder.AddIpAddress(IPAddress.IPv6Loopback);

      request.CertificateExtensions.Add(sanBuilder.Build());
      request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
         [new Oid("1.3.6.1.5.5.7.3.1")], false));

      var certificate = request.CreateSelfSigned(
         DateTimeOffset.UtcNow.AddDays(-1),
         DateTimeOffset.UtcNow.AddYears(1));

      return X509CertificateLoader.LoadPkcs12(
         certificate.Export(X509ContentType.Pfx),
         null,
         X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable);
   }
}

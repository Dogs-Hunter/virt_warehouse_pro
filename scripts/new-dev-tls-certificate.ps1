param([switch]$Force)

$ErrorActionPreference = "Stop"
$output = Join-Path (Split-Path -Parent $PSScriptRoot) "config\tls"
$certificatePath = Join-Path $output "warehouse.crt"
$keyPath = Join-Path $output "warehouse.key"
$bundlePath = Join-Path $output "warehouse.pem"
if (-not $Force -and (Test-Path $certificatePath) -and (Test-Path $keyPath) -and (Test-Path $bundlePath)) { return }

New-Item -ItemType Directory -Force $output | Out-Null
$rsa = [Security.Cryptography.RSA]::Create(2048)
$request = [Security.Cryptography.X509Certificates.CertificateRequest]::new(
    "CN=localhost", $rsa, [Security.Cryptography.HashAlgorithmName]::SHA256,
    [Security.Cryptography.RSASignaturePadding]::Pkcs1)
$san = [Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder]::new()
$san.AddDnsName("localhost")
$san.AddDnsName("nats-1")
$san.AddDnsName("nats-2")
$san.AddDnsName("nats-3")
$san.AddIpAddress([Net.IPAddress]::Loopback)
$request.CertificateExtensions.Add($san.Build())
$request.CertificateExtensions.Add([Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($false, $false, 0, $true))
$request.CertificateExtensions.Add([Security.Cryptography.X509Certificates.X509KeyUsageExtension]::new(
    ([Security.Cryptography.X509Certificates.X509KeyUsageFlags]::DigitalSignature -bor
     [Security.Cryptography.X509Certificates.X509KeyUsageFlags]::KeyEncipherment), $true))
$eku = [Security.Cryptography.OidCollection]::new()
[void]$eku.Add([Security.Cryptography.Oid]::new("1.3.6.1.5.5.7.3.1"))
[void]$eku.Add([Security.Cryptography.Oid]::new("1.3.6.1.5.5.7.3.2"))
$request.CertificateExtensions.Add([Security.Cryptography.X509Certificates.X509EnhancedKeyUsageExtension]::new($eku, $true))
$certificate = $request.CreateSelfSigned([DateTimeOffset]::UtcNow.AddMinutes(-5), [DateTimeOffset]::UtcNow.AddYears(2))
$certPem = $certificate.ExportCertificatePem()
$keyPem = $rsa.ExportPkcs8PrivateKeyPem()
[IO.File]::WriteAllText($certificatePath, $certPem, [Text.UTF8Encoding]::new($false))
[IO.File]::WriteAllText($keyPath, $keyPem, [Text.UTF8Encoding]::new($false))
[IO.File]::WriteAllText($bundlePath, "$($certPem.TrimEnd())`n$($keyPem.TrimEnd())`n", [Text.UTF8Encoding]::new($false))

package app.migrator.csfle.service;

// Import required classes for file operations and SSL/security
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Factory class for creating SSL contexts used in secure MongoDB connections
 * Particularly important for KMIP (Key Management Interoperability Protocol) communications
 */
public class SSLContextFactory {

    /**
     * Creates an SSLContext with both key store and trust store configurations
     * @param keyStorePath Path to the keystore file (usually .p12 or .jks)
     * @param keyStorePassword Password to access the keystore
     * @param trustStorePath Path to the truststore file (usually .jks)
     * @param trustStorePassword Password to access the truststore
     * @param keyStoreType Type of keystore (e.g., "PKCS12", "JKS")
     * @param trustStoreType Type of truststore (e.g., "JKS")
     * @return Configured SSLContext for secure communications
     */
    public static SSLContext create(
            String keyStorePath,
            String keyStorePassword,
            String trustStorePath,
            String trustStorePassword,
            String keyStoreType,
            String trustStoreType) throws Exception {

        if (keyStorePath == null || trustStorePath == null) {
            throw new IllegalArgumentException("KeyStore and TrustStore paths must not be null.");
        }

        System.out.println("[SSLContextFactory] Loading keystore: " + keyStorePath);
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        try (FileInputStream keyStoreInput = new FileInputStream(keyStorePath)) {
            keyStore.load(keyStoreInput, keyStorePassword.toCharArray());
        }

        System.out.println("[SSLContextFactory] Loading truststore: " + trustStorePath);
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        try (FileInputStream trustStoreInput = new FileInputStream(trustStorePath)) {
            trustStore.load(trustStoreInput, trustStorePassword.toCharArray());
        }

        Enumeration<String> aliases = trustStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            Certificate cert = trustStore.getCertificate(alias);
            System.out.println("Truststore contains: " + alias + " -> " + ((X509Certificate) cert).getSubjectX500Principal());
        }

        // Initialize KeyManagerFactory with client keystore for client authentication
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyStorePassword.toCharArray());

        // Initialize TrustManagerFactory with truststore for server certificate validation
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        // Create and initialize SSLContext with both managers
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

        System.out.println("[SSLContextFactory] SSLContext initialized successfully.");
        return sslContext;
    }
}

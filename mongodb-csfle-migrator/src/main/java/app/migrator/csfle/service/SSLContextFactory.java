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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating SSL contexts used in secure MongoDB connections
 * Particularly important for KMIP (Key Management Interoperability Protocol) communications
 */
public class SSLContextFactory {

    /**
     * Creates an SSLContext with optional key store and trust store configurations.
     * Either keystore, truststore, or both can be provided.
     *
     * @param keyStorePath Path to the keystore file (usually .p12 or .jks), can be null
     * @param keyStorePassword Password to access the keystore, can be null if keyStorePath is null
     * @param trustStorePath Path to the truststore file (usually .jks), can be null
     * @param trustStorePassword Password to access the truststore, can be null if trustStorePath is null
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

        final Logger logger = LoggerFactory.getLogger(SSLContextFactory.class);

        // Log warning if both are null, as this will result in default trust behavior
        if ((keyStorePath == null || keyStorePath.isEmpty()) &&
            (trustStorePath == null || trustStorePath.isEmpty())) {
            logger.warn("Both KeyStore and TrustStore paths are null or empty. Using default trust settings.");
        }

        // Create and initialize SSLContext
        SSLContext sslContext = SSLContext.getInstance("TLS");

        // Initialize KeyManagerFactory with client keystore for client authentication (optional)
        KeyManagerFactory kmf = null;

        // Initialize TrustManagerFactory with truststore for server certificate validation (optional)
        TrustManagerFactory tmf = null;

        // Process keystore if provided
        if (keyStorePath != null && !keyStorePath.isEmpty()) {
            try {
                logger.debug("Loading keystore: " + keyStorePath);
                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (FileInputStream keyStoreInput = new FileInputStream(keyStorePath)) {
                    char[] keyStorePasswordChars = keyStorePassword != null ? keyStorePassword.toCharArray() : null;
                    keyStore.load(keyStoreInput, keyStorePasswordChars);

                    kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(keyStore, keyStorePasswordChars);
                    logger.debug("KeyManagerFactory initialized successfully");
                }
            } catch (Exception e) {
                logger.error("Failed to load or initialize keystore: " + e.getMessage(), e);
                throw new IllegalStateException("Keystore initialization failed", e);
            }
        } else {
            logger.debug("No keystore provided, client authentication will not be used");
        }

        // Process truststore if provided
        if (trustStorePath != null && !trustStorePath.isEmpty()) {
            try {
                logger.debug("Loading truststore: " + trustStorePath);
                KeyStore trustStore = KeyStore.getInstance(trustStoreType);
                try (FileInputStream trustStoreInput = new FileInputStream(trustStorePath)) {
                    char[] trustStorePasswordChars = trustStorePassword != null ? trustStorePassword.toCharArray() : null;
                    trustStore.load(trustStoreInput, trustStorePasswordChars);

                    // Log certificate details for debugging
                    Enumeration<String> aliases = trustStore.aliases();
                    while (aliases.hasMoreElements()) {
                        String alias = aliases.nextElement();
                        Certificate cert = trustStore.getCertificate(alias);
                        if (cert instanceof X509Certificate) {
                            logger.debug("Truststore contains: " + alias + " -> " +
                                    ((X509Certificate) cert).getSubjectX500Principal());
                        }
                    }

                    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(trustStore);
                    logger.debug("TrustManagerFactory initialized successfully");
                }
            } catch (Exception e) {
                logger.error("Failed to load or initialize truststore: " + e.getMessage(), e);
                throw new IllegalStateException("Truststore initialization failed", e);
            }
        } else {
            logger.debug("No truststore provided, default trust settings will be used");
        }

        // Initialize context with available managers (either can be null)
        sslContext.init(
            kmf != null ? kmf.getKeyManagers() : null,
            tmf != null ? tmf.getTrustManagers() : null,
            new SecureRandom()
        );

        logger.info("SSLContext initialized successfully with keyStore={}, trustStore={}",
                   kmf != null, tmf != null);
        return sslContext;
    }
}

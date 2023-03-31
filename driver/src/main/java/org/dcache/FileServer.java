package org.dcache;

import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import eu.emi.security.authn.x509.helpers.ssl.SSLTrustManager;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.DirectoryCertChainValidator;
import eu.emi.security.authn.x509.impl.PEMCredential;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.InetAccessHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.servlet.MultipartConfigElement;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.List;

/*
 * Jetty socket that offers different servlets for flushing and staging files
 * for the packer-part of Sapphire
 */

public final class FileServer {
    private final Server server;
    private static final Logger LOGGER = LoggerFactory.getLogger(SapphireDriver.class);

    public FileServer (String hostname, int port, String[] whitelist, String certfile, String keyfile) throws GeneralSecurityException, IOException {
        long maxFilesize = -1L;
        long maxRequestsize = -1L;
        int filesizeThreshold = 64*1024;
        int maxThreads = 100;
        int minThreads = 10;
        int idleTimeout = 120;

        QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);

        server = new Server(threadPool);

        HttpConfiguration httpConfiguration = new HttpConfiguration();
        httpConfiguration.addCustomizer(new SecureRequestCustomizer());

        HttpConnectionFactory http11 = new HttpConnectionFactory(httpConfiguration);
        SslContextFactory sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setSslContext(createSslContext(certfile, keyfile, new char[0], "/dev/null"));

        SslConnectionFactory tls = new SslConnectionFactory(sslContextFactory, http11.getProtocol());
        ServerConnector connector = new ServerConnector(server, tls, http11);

        connector.setHost(hostname);
        connector.setPort(port);
        server.addConnector(connector);

        ServletContextHandler handler = new ServletContextHandler(server, "/v1");

        ServletHolder stageServletHolder = new ServletHolder(new StageServlet());
        String location = Paths.get("").toString();
        MultipartConfigElement multipartConfigElement = new MultipartConfigElement(location, maxFilesize, maxRequestsize, filesizeThreshold);
        stageServletHolder.getRegistration().setMultipartConfig(multipartConfigElement);

        handler.addServlet(stageServletHolder, "/stage");
        handler.addServlet(FileServlet.class, "/flush");

        InetAccessHandler accessHandler = new InetAccessHandler();
        for(String ip : whitelist) {
            accessHandler.include(ip);
        }
        accessHandler.setHandler(server.getHandler());
        server.setHandler(accessHandler);
    }

    public void startServer() throws Exception {
        server.start();
        LOGGER.info("Sapphire-server started");
    }

    public void stopServer() throws Exception {
        server.stop();
        LOGGER.info("Sapphire-server stopped");
    }

    public static SSLContext createSslContext(
            String certificateFile, String certificateKeyFile, char[] keyPassword, String trustStore)
            throws IOException, GeneralSecurityException {

        // due to bug in canl https://github.com/eu-emi/canl-java/issues/100 enforce absolute path
        if (trustStore.charAt(0) != '/') {
            trustStore = new File(".", trustStore).getAbsolutePath();
        }

        X509CertChainValidatorExt certificateValidator =
                new DirectoryCertChainValidator(
                        List.of(trustStore), CertificateUtils.Encoding.PEM, -1, 5000, null);

        PEMCredential serviceCredentials =
                new PEMCredential(certificateKeyFile, certificateFile, keyPassword);

        KeyManager keyManager = serviceCredentials.getKeyManager();
        KeyManager[] kms = new KeyManager[]{keyManager};
        SSLTrustManager tm = new SSLTrustManager(certificateValidator);

        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(kms, new TrustManager[]{tm}, null);

        return sslCtx;
    }
}

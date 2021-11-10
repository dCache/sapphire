package org.dcache;

import com.google.common.util.concurrent.Futures;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.vehicles.FileAttributes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SapphireDriverTest {

    private static final String PNFS_ID = "00006EFBAFF13D2545159C03CBB903DFD19E";
    private static final String REQUEST_ID = "ced8e5d1-1319-4ca3-a979-1e421dd2b6b8";

    private MongoServer mongoServer;
    private MongoClient mongoClient;
    private MongoCollection<Document> fileCollection;

    private SapphireDriver sapphireDriver;
    private FlushRequest flushRequest;

    private File keyFile;
    private File certFile;

    @BeforeEach
    public void setUp() throws IOException, GeneralSecurityException, OperatorCreationException {
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        String connectionString = "mongodb://" + new ServerAddress(serverAddress).toString();
        mongoClient = MongoClients.create(connectionString);

        keyFile = File.createTempFile("hostkey-", ".pem");
        certFile = File.createTempFile("hostcert-", ".pem");

        generateSelfSignedCert();

        sapphireDriver = new SapphireDriver("foo", "bar");

        Map<String, String> config = Map.of(
                "mongo_url", connectionString,
                "database", "hsm",
                "period", "1",
                "period_unit", TimeUnit.SECONDS.name(),
                "port", "12300",
                "cert", certFile.getAbsolutePath(),
                "key", keyFile.getAbsolutePath()
        );

        fileCollection = mongoClient.getDatabase("hsm").getCollection("files");
        sapphireDriver.configure(config);

        flushRequest = mock(FlushRequest.class);
        StorageInfo si = GenericStorageInfo.valueOf("A:B@C", "*");
        si.setKey("path", "/some/dcache/path/file1");
        when(flushRequest.getFileAttributes()).thenReturn(
                FileAttributes.of()
                        .pnfsId(PNFS_ID)
                        .size(123)
                        .storageInfo(si)
                        .build()
        );
        when(flushRequest.getReplicaUri()).thenReturn(URI.create("/some/dcache/path/file1"));
        when(flushRequest.activate()).thenReturn(Futures.immediateFuture(null));
        when(flushRequest.getId()).thenReturn(UUID.fromString(REQUEST_ID));
    }

    @Test
    public void shouldActiveFlushRequestOnSubmit() {
        sapphireDriver.flush(Set.of(flushRequest));
        verify(flushRequest).activate();
    }

    @Test
    public void shouldPopulateDb() {
        sapphireDriver.flush(Set.of(flushRequest));
        waitForDriverRun(2);

        assertNotNull(fileCollection.find(eq("pnfsid", PNFS_ID)).first(), "mongo db is not populated");
    }

    @Test
    public void shouldSuccessFlushWhenUrlProvided() {
        sapphireDriver.flush(Set.of(flushRequest));

        waitForDriverRun(2);

        fileCollection.updateOne(eq("pnfsid", PNFS_ID),
                new Document("$set", new BasicBSONObject().append("archiveUrl",  "dcache://dcache/123:456")));

        waitForDriverRun(2);
        verify(flushRequest).completed(anySet());
        assertNull(fileCollection.find(eq("pnfsid", PNFS_ID)).first(), "Completed entry not removed");
    }

    @Test
    public void shouldFailFlushOnBadUrl() {
        sapphireDriver.flush(Set.of(flushRequest));

        waitForDriverRun(2);

        fileCollection.updateOne(eq("pnfsid", PNFS_ID),
                new Document("$set", new BasicBSONObject().append("archiveUrl",  "123:456")));

        waitForDriverRun(2);
        verify(flushRequest).failed(any(URISyntaxException.class));
        assertNull(fileCollection.find(eq("pnfsid", PNFS_ID)).first(), "Failed entry not removed");
    }

    @Test
    public void shouldFailOnCancelRequest() {
        sapphireDriver.flush(Set.of(flushRequest));

        waitForDriverRun(2);

        sapphireDriver.cancel(UUID.fromString(REQUEST_ID));
        verify(flushRequest).failed(any(CancellationException.class));
        assertNull(fileCollection.find(eq("pnfsid", PNFS_ID)).first(), "Canceled entry not removed");
    }

    @Test
    public void shouldNotCancelForRandomID() {
        sapphireDriver.flush(Set.of(flushRequest));

        waitForDriverRun(2);

        sapphireDriver.cancel(UUID.randomUUID());
        assertNotNull(fileCollection.find(eq("pnfsid", PNFS_ID)).first(), "Should not remove when UUID doesn't match");
    }

    @AfterEach
    public void tearDown() {
        sapphireDriver.shutdown();
        mongoClient.close();
        mongoServer.shutdownNow();
    }

    private void waitForDriverRun(int sec) {
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (InterruptedException e) {}
    }


    private void generateSelfSignedCert()
            throws GeneralSecurityException, OperatorCreationException, IOException {

        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", new BouncyCastleProvider());
        keyPairGenerator.initialize(2048, new SecureRandom());
        KeyPair keyPair = keyPairGenerator.generateKeyPair();

        long notBefore = System.currentTimeMillis();
        long notAfter = notBefore + TimeUnit.DAYS.toMillis(1);

        X500Name subjectDN = new X500Name("CN=localhost, O=dCache.org");
        X500Name issuerDN = subjectDN;

        SubjectPublicKeyInfo subjectPublicKeyInfo =
                SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

        X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(issuerDN,
                BigInteger.ONE,
                new Date(notBefore),
                new Date(notAfter), subjectDN,
                subjectPublicKeyInfo)
                .addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
                .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment))
                .addExtension(Extension.extendedKeyUsage, true, new ExtendedKeyUsage(
                                new KeyPurposeId[] {KeyPurposeId.id_kp_clientAuth, KeyPurposeId.id_kp_serverAuth}
                        )
                );

        String signatureAlgorithm = "SHA256WithRSA";

        // sign with own key
        ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm)
                .build(keyPair.getPrivate());

        X509CertificateHolder certificateHolder = certificateBuilder.build(contentSigner);
        var cert = new JcaX509CertificateConverter().getCertificate(certificateHolder);

        try (OutputStream certOut = Files.newOutputStream(
                certFile.toPath(), CREATE, TRUNCATE_EXISTING,
                WRITE); OutputStream keyOut = Files.newOutputStream(keyFile.toPath(), CREATE,
                TRUNCATE_EXISTING, WRITE)) {

            CertificateUtils.saveCertificate(certOut, cert, CertificateUtils.Encoding.PEM);
            CertificateUtils.savePrivateKey(keyOut, keyPair.getPrivate(), CertificateUtils.Encoding.PEM, null, null);
        }
    }
}

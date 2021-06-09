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
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.vehicles.FileAttributes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SapphireDriverTest {

    private static final String PNFS_ID = "00006EFBAFF13D2545159C03CBB903DFD19E";
    private static final String REQUEST_ID = "ced8e5d1-1319-4ca3-a979-1e421dd2b6b8";

    private MongoServer mongoServer;
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    private SapphireDriver sapphireDriver;
    private FlushRequest request;

    @BeforeEach
    public void setUp() {
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        String connectionString = "mongodb://" + new ServerAddress(serverAddress).toString();
        mongoClient = MongoClients.create(connectionString);

        sapphireDriver = new SapphireDriver("foo", "bar");

        Map<String, String> config = Map.of(
                "mongo_url", connectionString,
                "database", "hsm",
                "period", "1",
                "period_unit", TimeUnit.SECONDS.name(),
                "port", "12300-12500"
        );

        collection = mongoClient.getDatabase("hsm").getCollection("files");
        sapphireDriver.configure(config);

        request = mock(FlushRequest.class);
        StorageInfo si = GenericStorageInfo.valueOf("A:B@C", "*");
        si.setKey("path", "/some/dcache/path/file1");
        when(request.getFileAttributes()).thenReturn(
                FileAttributes.of()
                        .pnfsId(PNFS_ID)
                        .size(123)
                        .storageInfo(si)
                        .build()
        );
        when(request.getReplicaUri()).thenReturn(URI.create("/some/dcache/path/file1"));
        when(request.activate()).thenReturn(Futures.immediateFuture(null));
        when(request.getId()).thenReturn(UUID.fromString(REQUEST_ID));
    }

    @Test
    public void shouldActiveRequestOnSubmit() {
        sapphireDriver.flush(Set.of(request));
        verify(request).activate();
    }

    @Test
    public void shouldPopulateDb() {
        sapphireDriver.flush(Set.of(request));
        waitForDriverRun(2);

        assertNotNull(collection.find(eq("pnfsid", PNFS_ID)).first(), "mongo db is not populated");
    }

    @Test
    public void shouldSuccessWhenUrlProvided() {
        sapphireDriver.flush(Set.of(request));

        waitForDriverRun(2);

        collection.updateOne(eq("pnfsid", PNFS_ID),
                new Document("$set", new BasicBSONObject().append("archiveUrl",  "dcache://dcache/123:456")));

        waitForDriverRun(2);
        verify(request).completed(anySet());
        assertNull(collection.find(eq("pnfsid", PNFS_ID)).first(), "Completed entry not removed");
    }

    @Test
    public void shouldFailOnBadUrl() {
        sapphireDriver.flush(Set.of(request));

        waitForDriverRun(2);

        collection.updateOne(eq("pnfsid", PNFS_ID),
                new Document("$set", new BasicBSONObject().append("archiveUrl",  "123:456")));

        waitForDriverRun(2);
        verify(request).failed(any(URISyntaxException.class));
        assertNull(collection.find(eq("pnfsid", PNFS_ID)).first(), "Failed entry not removed");
    }

    @Test
    public void shouldFailOnCancelRequest() {
        sapphireDriver.flush(Set.of(request));

        waitForDriverRun(2);

        sapphireDriver.cancel(UUID.fromString(REQUEST_ID));
        verify(request).failed(any(CancellationException.class));
        assertNull(collection.find(eq("pnfsid", PNFS_ID)).first(), "Canceled entry not removed");
    }

    @Test
    public void shouldNotCancelForRandomID() {
        sapphireDriver.flush(Set.of(request));

        waitForDriverRun(2);

        sapphireDriver.cancel(UUID.randomUUID());
        assertNotNull(collection.find(eq("pnfsid", PNFS_ID)).first(), "Should not remove when UUID doesn't match");
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
}

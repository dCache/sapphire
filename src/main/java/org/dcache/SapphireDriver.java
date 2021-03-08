package org.dcache;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;

import static com.mongodb.client.model.Filters.*;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.client.*;
import org.bson.Document;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SapphireDriver implements NearlineStorage
{
    private static final Logger _log = LoggerFactory.getLogger(SapphireDriver.class);

    protected final String type;
    protected final String name;
    private MongoClient mongoClient;
    MongoCollection<Document> files;
    private final Queue<FlushRequest> flushRequestQueue;
    private final ScheduledExecutorService executorService;

    public SapphireDriver(String type, String name)
    {
        this.type = type;
        this.name = name;
        flushRequestQueue = new ConcurrentLinkedDeque<>();
        executorService = new ScheduledThreadPoolExecutor(1);
        executorService.scheduleAtFixedRate(this::processFlush, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * Flush all files in {@code requests} to nearline storage.
     *
     * @param requests
     */
    @Override
    public void flush(Iterable<FlushRequest> requests)
    {
        _log.debug("Triggered flush()");

        for(FlushRequest flushRequest : requests) {
            flushRequest.activate();
            _log.debug("Added file to flushRequestQueue");
            flushRequestQueue.add(flushRequest);
        }
        _log.debug("Length of flushRequestQueue: {}", flushRequestQueue.size());
    }

    /**
     * Stage all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Delete all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void remove(Iterable<RemoveRequest> requests)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Cancel any flush, stage or remove request with the given id.
     * <p>
     * The failed method of any cancelled request should be called with a
     * CancellationException. If the request completes before it can be
     * cancelled, then the cancellation should be ignored and the completed
     * or failed method should be called as appropriate.
     * <p>
     * A call to cancel must be non-blocking.
     *
     * @param uuid id of the request to cancel
     */
    @Override
    public void cancel(UUID uuid)
    {
        _log.debug("Cancel triggered for UUID {}", uuid);
        Predicate<FlushRequest> byUUID = request -> request.getId().equals(uuid);
        flushRequestQueue.stream().filter(byUUID)
                .findAny()
                .ifPresent(request ->  {
                    if (flushRequestQueue.removeIf(byUUID)) {
                        files.deleteOne(new Document("pnfsid", request.getFileAttributes().getPnfsId().toString()));
                        request.failed(new CancellationException());
                    }
                });
    }

    /**
     * Applies a new configuration.
     *
     * @param properties
     * @throws IllegalArgumentException if the configuration is invalid
     */
    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        _log.debug("Triggered configure()");
        String mongoUri = properties.getOrDefault("mongo_url", "");
        String database = properties.getOrDefault("database", "");

        if (mongoUri.equals("") || database.equals("")) {
            String propertiesPath = properties.getOrDefault("conf_file", "");
            if (propertiesPath.equals("")) {
                throw new IllegalArgumentException("No or not enough details to MongoDB or configuration file given.");
            } else {
                try(InputStream inputStream = new FileInputStream(propertiesPath)){
                    Properties prop = new Properties();
                    try{
                        prop.load(inputStream);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    mongoUri = prop.getProperty("mongo_url");
                    database = prop.getProperty("database");

                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Configuration file not found");
                } catch (IOException e) {
                    throw new RuntimeException("Could not open and read configuration file");
                }
            }
        }
        _log.debug("mongoUri: {}; database: {}", mongoUri, database);

        try{
            mongoClient = MongoClients.create(mongoUri);
        } catch (MongoSocketOpenException e) {
            _log.error("Could not open connection to MongoDB");
            throw e;
        }
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        files = mongoDatabase.getCollection("files");
    }

    /**
     * Cancels all requests and initiates a shutdown of the nearline storage
     * interface.
     * <p>
     * This method does not wait for actively executing requests to
     * terminate.
     */
    @Override
    public void shutdown()
    {
        mongoClient.close();
        executorService.shutdown();
    }

    private void processFlush() {
        _log.debug("processFlush() called");
        Queue<FlushRequest> notYetReady = new ArrayDeque<>();
        FlushRequest request;

        while ((request = flushRequestQueue.poll()) != null) {

            String pnfsid = request.getFileAttributes().getPnfsId().toString();
            _log.debug("PNFSID: {}", pnfsid);

            if (request.getFileAttributes().getSize() == 0) {
                _log.debug("Filesize is 0");
                String store = request.getFileAttributes().getStorageInfo().getKey("store");
                String group = request.getFileAttributes().getStorageInfo().getKey("group");
                try {
                    request.completed(Collections.singleton(new URI("dcache://dcache/store=" + store +
                            "&group=" + group + "&bfid=" + pnfsid + ":*")));
                    continue;
                } catch (URISyntaxException e) {
                    throw new RuntimeException("Could not create URI to complete FlushRequest with filesize 0");
                }
            }

            FindIterable<Document> results = files.find(eq("pnfsid", pnfsid)).limit(1);
            Document result = results.first();
            if(result != null) {
                _log.debug("Result: {}", result.toJson());
                if (result.containsKey("archiveUrl")) {
                    try {
                        String archiveUrl = (String) result.get("archiveUrl");
                        URI fileUri = new URI(archiveUrl.replace("dcache://dcache", type + "://" + name));
                        _log.debug("archiveUrl exists, fileUri: {}", fileUri);
                        files.deleteOne(new Document("pnfsid", pnfsid));
                        request.completed(Collections.singleton(fileUri));
                    } catch (URISyntaxException e) {
                        _log.error("Error completing flushRequest: " + e);
                        request.failed(e);
                    }
                } else {
                    notYetReady.offer(request);
                }
            } else {
                Document entry = new Document("pnfsid", pnfsid)
                        .append("store", request.getFileAttributes().getStorageInfo().getKey("store"))
                        .append("group", request.getFileAttributes().getStorageInfo().getKey("group"));
                _log.debug("Inserting to database: {}", entry.toJson());
                files.insertOne(entry);
                notYetReady.offer(request);
            }
        }
        _log.debug("NotYetReady size: {} will be added to flushRequestQueue now", notYetReady.size());
        flushRequestQueue.addAll(notYetReady);
    }
}

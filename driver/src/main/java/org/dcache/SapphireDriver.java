package org.dcache;

import java.io.*;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.*;
import com.mongodb.client.*;
import diskCacheV111.util.Adler32;
import org.apache.commons.io.FileUtils;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.Document;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.FireAndForgetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SapphireDriver implements NearlineStorage
{
    private static final Logger _log = LoggerFactory.getLogger(SapphireDriver.class);

    protected final String type;
    protected final String name;
    private MongoClient mongoClient;
    MongoCollection<Document> files;
    MongoCollection<Document> stageFiles;
    private final Queue<FlushRequest> flushRequestQueue;
    private final Queue<StageRequest> stageRequestQueue;
    private final ScheduledExecutorService executorService;
    private FileServer server;

    public SapphireDriver(String type, String name)
    {
        this.type = type;
        this.name = name;
        flushRequestQueue = new ConcurrentLinkedDeque<>();
        stageRequestQueue = new ConcurrentLinkedDeque<>();

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("sapphire-nearline-storage-%d")
                .setUncaughtExceptionHandler(this::uncaughtException)
                .build();

        executorService = new ScheduledThreadPoolExecutor(1, threadFactory);
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
            try {
                flushRequest.activate().get();
                flushRequestQueue.add(flushRequest);
                _log.debug("Added {} to flushRequestQueue", flushRequest.getFileAttributes().getPnfsId());
            } catch (ExecutionException | InterruptedException e) {
                Throwable t = Throwables.getRootCause(e);
                _log.error("Failed to activate request {}", t.getMessage());
                flushRequest.failed(e);
            }
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
        _log.debug("Stage triggered");

        for(StageRequest stageRequest : requests) {
            try {
                stageRequest.activate().get();
                stageRequestQueue.add(stageRequest);
                _log.debug("Added {} to stageRequestQueue", stageRequest.getFileAttributes().getPnfsId());
            } catch (ExecutionException | InterruptedException e) {
                Throwable t = Throwables.getRootCause(e);
                _log.error("Failed to activate request {}", t.getMessage());
                stageRequest.failed(e);
            }
        }
        _log.debug("Length of stageRequestQueue: {}", stageRequestQueue.size());
    }

    private Checksum calculateAdler32(File file) {
        Adler32 newChecksum = new Adler32();
        try {
            byte [] fileArray = FileUtils.readFileToByteArray(file);
            newChecksum.engineUpdate(fileArray, 0, fileArray.length);
        } catch (IOException e) {
            _log.error("Could not calculate checksum for file {}: ", file.getPath(), e);
            return null;
        }
        return new Checksum(ChecksumType.ADLER32, newChecksum.engineDigest());
    }

    private Checksum calculateMd5(File file) { // TODO Needs testing
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            InputStream in = new DataInputStream(new FileInputStream(file));
            DigestInputStream din = new DigestInputStream(in, md);
            _log.debug("Value of DigestInputStream MD5: {}", din.read());
        } catch (NoSuchAlgorithmException e) {
            _log.error("Could not calculate checksum MD5, ", e);
        } catch (FileNotFoundException e) {
            _log.error("File not found for MD5 calculation");
        } catch (IOException e) {
            _log.error("IOException while reading: ", e);
        }

        if (md == null) {
            return null;
        } else {
            return new Checksum(ChecksumType.MD5_TYPE, md.digest());
        }
    }

    private void processStage() {
        _log.debug("processStage() called");
        Queue<StageRequest> notYetReady = new ArrayDeque<>();
        StageRequest request;
        String pnfsid;
        File file;

        while ((request = stageRequestQueue.poll()) != null) {
            pnfsid = request.getFileAttributes().getPnfsId().toString();
            file = new File(request.getReplicaUri());
            Document result = null;
            _log.debug("Found request for file {} {}", pnfsid, file.getPath());
            try {
                    FindIterable<Document> results = stageFiles.find(new Document("pnfsid", pnfsid));
                    result = results.first();
            } catch (MongoException e) {
                _log.error("An error occured while requesting MongoDB to find files to be staged: ", e);
                notYetReady.add(request);
                continue;
            }

            if (result != null && result.get("status").equals("done") && file.exists()) {
                _log.debug("File {} exists", request.getFileAttributes().getPnfsId());
                Optional<Set<Checksum>> requestChecksum = request.getFileAttributes().getChecksumsIfPresent();

                if (requestChecksum.isPresent()) {
                    Set<Checksum> requestChecksums = requestChecksum.get();
                    boolean checksumFound = false;

                    for (Checksum checksum : requestChecksums) {
                        _log.debug("Request checksum: {}", checksum.toString());

                        ChecksumType checksumType = checksum.getType();
                        Checksum newChecksum = null;

                        if (checksumType.equals(ChecksumType.ADLER32)) {
                            newChecksum = calculateAdler32(file);
                        } else if (checksumType.equals(ChecksumType.MD5_TYPE)) {
                            newChecksum = calculateMd5(file);
                        }

                        _log.debug("New checksum: {}", newChecksum != null ? newChecksum.toString() : "null");

                        if (newChecksum != null && newChecksum.equals(checksum)) {
                            _log.debug("Checksums are equal: {} ; {}", requestChecksum, newChecksum);

                            request.completed(Collections.singleton(newChecksum));
                            stageFiles.deleteOne(new Document("pnfsid", pnfsid));

                            _log.info("Stage for file {} finished successfully", pnfsid);
                            checksumFound = true;
                            break;
                        } else {
                            _log.error((newChecksum == null ? "Could not calculate checksum" : "Checksums are not equal")
                                    + " for file {}", pnfsid);
                        }
                    }
                    if (!checksumFound) {
                        _log.error("No checksum could be calculated or matched the original checksum. Deleting the " +
                                "file and set the MongoDB record to stage the file again!");
                        if (!file.delete()) {
                            _log.error("Could not delete file {} {}", pnfsid, file.getPath());
                        }
                        stageFiles.updateOne(new Document("pnfsid", pnfsid), new Document("status", "new"));
                    }
                } else {
                    _log.warn("There is no Checksum for file {} in dCache. " +
                            "File is staged without checksum comparison!", pnfsid);
                    Set<Checksum> checksums = new HashSet<>();
                    checksums.add(calculateAdler32(file));
                    checksums.add(calculateMd5(file));
                    request.completed(checksums);
                }
            } else if (result != null && result.get("status").equals("failure")) {
                _log.error("Staging the file failed on packer side. Please look into logs of stage-files.py on the packing node for more information!");
                stageFiles.deleteOne(new Document("pnfsid", pnfsid));
                request.failed(new FileNotFoundException("Unable to stage file"));
            } else {
                _log.debug("File not found {}", pnfsid);

                if (result == null) {
                    _log.debug("Add MongoDB Record");
                    try {
                        List<URI> locations = null;
                        try {
                            _log.debug("Location for file {}: {}", pnfsid, request.getFileAttributes().getStorageInfo().locations().get(0).toString());
                            locations = request.getFileAttributes().getStorageInfo().locations();
                        } catch (IndexOutOfBoundsException e) {
                            _log.error("There are no locations available for file {}! ", pnfsid,  e);
                            request.failed(e);
                            continue;
                        } catch (IllegalStateException e) {
                            _log.error("Could not get StorageInfo for file {}! ", pnfsid, e);
                            request.failed(e);
                            continue;
                        }
                        Document record = new Document();

                        List<String> locationList = locations.stream().map(URI::toString).collect(Collectors.toList());
                        List<BsonString> bsonLocations = locationList.stream().map(BsonString::new).collect(Collectors.toList());


                        record.append("pnfsid", pnfsid)
                              .append("filepath", request.getReplicaUri().getPath())
                              .append("locations", new BsonArray(bsonLocations))
                              .append("status", "new");

                        stageFiles.insertOne(record);
                        request.allocate();
                        _log.info("Start staging process for file {}", pnfsid);
                    } catch (MongoException e) {
                        _log.error("Record for file {} could not be written to MongoDB", pnfsid, e);
                    }
                } else {
                    _log.debug("MongoDB record exists");
                }
                notYetReady.add(request);
            }
        }
        _log.debug("NotYetReady size: {} will be added to stageRequestQueue now", notYetReady.size());
        stageRequestQueue.addAll(notYetReady);

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
        Predicate<FlushRequest> flushByUUID = request -> request.getId().equals(uuid);
        Predicate<StageRequest> stageByUUID = request -> request.getId().equals(uuid);

        flushRequestQueue.stream().filter(flushByUUID)
                .findAny()
                .ifPresent(request ->  {
                    if (flushRequestQueue.removeIf(flushByUUID)) {
                        files.deleteOne(new Document("pnfsid", request.getFileAttributes().getPnfsId().toString()));
                        request.failed(new CancellationException());
                    }
                });

        stageRequestQueue.stream().filter(stageByUUID)
                .findAny()
                .ifPresent(request -> {
                    if (stageRequestQueue.removeIf(stageByUUID)) {
                        stageFiles.deleteOne(new Document("pnfsid", request.getFileAttributes().getPnfsId().toString()));
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
        stageFiles = mongoDatabase.getCollection("stage");

        long schedulerPeriod = Long.parseLong(properties.getOrDefault("period", "1"));
        TimeUnit periodUnit = TimeUnit.valueOf(properties.getOrDefault("period_unit", TimeUnit.MINUTES.name()));

        executorService.scheduleAtFixedRate(new FireAndForgetTask(this::processFlush), schedulerPeriod, schedulerPeriod, periodUnit);
        executorService.scheduleAtFixedRate(new FireAndForgetTask(this::processStage), schedulerPeriod, schedulerPeriod, periodUnit);

        String[] whitelist = properties.getOrDefault("whitelist", "").split(",");
        String portStr = properties.getOrDefault("port", "");

        int port;
        if(portStr.contains("-")) {
            _log.debug("Port range is used");
            port = getFreePort(Integer.parseInt(portStr.split("-")[0]),
                    Integer.parseInt(portStr.split("-")[1]));
        } else {
            _log.debug("Static port is used");
            port = Integer.parseInt(portStr);
        }
        _log.info("Sapphire is running on port {}", port);

        try {
            server = new FileServer(port, whitelist);
            server.startServer();
        } catch (Exception e) {
            _log.error("Could not start Jetty server", e);
            throw new RuntimeException(e);
        }
    }

    private int getFreePort(int start, int end) {
        _log.debug("Trying to find free port between {} and {}", start, end);
        int port = start;
        while (port < end) {
            _log.debug("Trying port {}", port);
            try {
                new ServerSocket(port).close();
                _log.debug("Port {} is free", port);
                return port;
            } catch (IOException e) {
                _log.debug("Port {} is already used", port);
                port += 1;
            }
        }
        return -1;
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
        if (mongoClient != null) {
            mongoClient.close();
        }
        executorService.shutdown();
        try {
            if (server != null) {
                server.stopServer();
            }
        } catch (Exception e) {
            _log.error("Error stopping Jetty server", e);
        }
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
                        files.deleteOne(new Document("pnfsid", pnfsid));
                        String archiveUrl = (String) result.get("archiveUrl");
                        URI fileUri = new URI(archiveUrl.replace("dcache://dcache", type + "://" + name));
                        _log.debug("archiveUrl exists, fileUri: {}", fileUri);
                        _log.info("Location for file {} is {}", pnfsid, fileUri);
                        request.completed(Collections.singleton(fileUri));
                    } catch (URISyntaxException e) {
                        _log.error("Error completing flushRequest: " + e);
                        request.failed(e);
                    }
                } else {
                    notYetReady.offer(request);
                }
            } else {
                Path path = Path.of(request.getFileAttributes().getStorageInfo().getKey("path"));
                _log.debug("Path: " + request.getFileAttributes().getStorageInfo());
                Document entry = new Document("pnfsid", pnfsid)
                        .append("store", request.getFileAttributes().getStorageInfo().getKey("store"))
                        .append("group", request.getFileAttributes().getStorageInfo().getKey("group"))
                        .append("path", path.toString())
                        .append("parent", path.getParent().toString())
                        .append("replica_uri", request.getReplicaUri().getPath())
                        .append("size", request.getFileAttributes().getSize())
                        .append("ctime", Double.parseDouble(Long.toString(request.getReplicaCreationTime())) / 1000)
                        .append("hsm_type", this.type)
                        .append("hsm_name", this.name)
                        .append("state", "new");
                _log.debug("Inserting to database: {}", entry.toJson());
                files.insertOne(entry);
                notYetReady.offer(request);
            }
        }
        _log.debug("NotYetReady size: {} will be added to flushRequestQueue now", notYetReady.size());
        flushRequestQueue.addAll(notYetReady);
    }

    private void uncaughtException(Thread t, Throwable e) {
        _log.error("Uncaught exception in {}", t.getName(), e);
    }
}

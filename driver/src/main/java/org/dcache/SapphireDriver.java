package org.dcache;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
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
import org.apache.commons.codec.binary.Hex;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.Document;

import org.dcache.pool.nearline.spi.*;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.FireAndForgetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class is the core of Sapphire. It handles Stage- and FlushRequests on
 * the driver-side of Sapphire
 */

public class SapphireDriver implements NearlineStorage
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SapphireDriver.class);

    protected final String type;
    protected final String name;

    private MongoClient mongoClient;
    private String mongoUri;
    private String database;
    private MongoCollection<Document> files;
    private MongoCollection<Document> stageFiles;

    private final Queue<FlushRequest> flushRequestQueue;
    private final Queue<StageRequest> stageRequestQueue;
    private final ScheduledExecutorService executorService;

    private FileServer server;
    private long schedulerPeriod;
    private TimeUnit periodUnit;
    private String [] whitelist;
    private int port;
    private String hostname;
    private String certfile;
    private String keyfile;
    private boolean stageQueueLocked = false;
    private boolean flushQueueLocked = false;
    private boolean cancelRequest = false;

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

    @Override
    public void start() {
        LOGGER.debug("Triggered start()");
        try{
            mongoClient = MongoClients.create(mongoUri);
        } catch (MongoSocketOpenException e) {
            LOGGER.error("Could not open connection to MongoDB");
            throw e;
        }
        MongoDatabase mongoDatabase = mongoClient.getDatabase(this.database);
        files = mongoDatabase.getCollection("files");
        stageFiles = mongoDatabase.getCollection("stage");

        try {
            server = new FileServer(hostname, port, whitelist, certfile, keyfile);
            server.startServer();
        } catch (Exception e) {
            LOGGER.error("Could not start Jetty server", e);
            throw new RuntimeException(e);
        }

        executorService.scheduleAtFixedRate(new FireAndForgetTask(this::processFlush), schedulerPeriod, schedulerPeriod, periodUnit);
        executorService.scheduleAtFixedRate(new FireAndForgetTask(this::processStage), schedulerPeriod, schedulerPeriod, periodUnit);
    }

    /**
     * Applies a new configuration.
     *
     * @param properties
     * @throws IllegalArgumentException if the configuration is invalid
     */
    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException {
        LOGGER.debug("Triggered configure()");

        try {
            this.mongoUri = properties.get("mongo_url");
            this.database = properties.get("database");
            this.schedulerPeriod = Long.parseLong(properties.getOrDefault("period", "1"));
            this.periodUnit = TimeUnit.valueOf(properties.getOrDefault("period_unit", TimeUnit.MINUTES.name()));
            this.whitelist = properties.getOrDefault("whitelist", "").split(",");
            this.port = Integer.parseInt(properties.getOrDefault("port", ""));
            this.certfile = properties.getOrDefault("cert", "/etc/grid-security/hostcert.pem");
            this.keyfile = properties.getOrDefault("key", "/etc/grid-security/hostkey.pem");
            this.hostname = InetAddress.getLocalHost().getHostName();
        } catch (NullPointerException e) {
            LOGGER.error("There's a mandatory parameter missing.", e);
        } catch (UnknownHostException e)
        {
            LOGGER.error("Could not get Hostname");
            throw new RuntimeException(e);
        }
    }


    /**
     * Flush all files in {@code requests} to nearline storage.
     *
     * @param requests
     */
    @Override
    public void flush(Iterable<FlushRequest> requests)
    {
        LOGGER.debug("Triggered flush()");

        for(FlushRequest flushRequest : requests) {
            try {
                flushRequest.activate().get();
                flushRequestQueue.add(flushRequest);
                LOGGER.debug("Added {} to flushRequestQueue", flushRequest.getFileAttributes().getPnfsId());
            } catch (ExecutionException | InterruptedException e) {
                Throwable t = Throwables.getRootCause(e);
                LOGGER.error("Failed to activate request {}", t.getMessage());
                flushRequest.failed(e);
            }
        }
        LOGGER.debug("Length of flushRequestQueue: {}", flushRequestQueue.size());
    }


    /**
     * Stage all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        LOGGER.debug("Stage triggered");

        for(StageRequest stageRequest : requests) {
            try {
                stageRequest.activate().get();
                stageRequestQueue.add(stageRequest);
                newStageRequest(stageRequest);
                LOGGER.debug("Added {} to stageRequestQueue", stageRequest.getFileAttributes().getPnfsId());
            } catch (ExecutionException | InterruptedException e) {
                Throwable t = Throwables.getRootCause(e);
                LOGGER.error("Failed to activate request {}", t.getMessage());
                stageRequest.failed(e);
            }
        }
        LOGGER.debug("Length of stageRequestQueue: {}", stageRequestQueue.size());
    }

    private Checksum calculateAdler32(File file) throws IOException {
        LOGGER.error("Adler32 Checksum calculation");
        Adler32 newChecksum = new Adler32();
        byte [] buffer = new byte[4096];
        try (InputStream in = new FileInputStream(file)) {
            int len;
            while ((len = in.read(buffer)) > 0) {
                newChecksum.update(buffer, 0, len);
            }
        }
        return new Checksum(ChecksumType.ADLER32, newChecksum.engineDigest());
    }

    private Checksum calculateMd5(File file) throws NoSuchAlgorithmException, FileNotFoundException, IOException {
        LOGGER.error("MD5 Checksum calculation");
        MessageDigest md;
        md = MessageDigest.getInstance("MD5");
        byte [] buffer = new byte[4096];
        try (FileInputStream fin = new FileInputStream(file)) {
            int len;
            while ((len = fin.read(buffer)) > 0) {
                md.update(buffer, 0, len);
            }
            md.update(fin.readAllBytes());
        }
        String checksum = Hex.encodeHexString(md.digest());
        return new Checksum(ChecksumType.MD5_TYPE, checksum);
    }

    private void resetFile(String pnfsid, File file) {
        if (!file.delete()) {
            LOGGER.error("Could not delete file {} {}", pnfsid, file.getPath());
        }
        stageFiles.updateOne(new Document("pnfsid", pnfsid), new Document("$set", new Document("status", "new")));
    }

    private Checksum compareAndReturnChecksum(Set<Checksum> originalChecksums, File file) throws FileNotFoundException{
        for (Checksum checksum : originalChecksums) {
            LOGGER.debug("Request checksum: {}", checksum.toString());

            ChecksumType checksumType = checksum.getType();
            Checksum newChecksum = null;

            switch (checksumType) {
                case ADLER32:
                    try {
                        newChecksum = calculateAdler32(file);
                    } catch (IOException e) {
                        LOGGER.error("Could not calculate Adler32 Checksum for file {} due to an IOException: {}",
                            file.getPath(), e);
                        continue;
                    }
                    break;
                case MD5_TYPE:
                    try {
                        newChecksum = calculateMd5(file);
                    } catch (NoSuchAlgorithmException e) {
                        LOGGER.error("Can't calculate MD5 checksum, no algorithm for MD5 found");
                        continue;
                    } catch (IOException e) {
                        LOGGER.error("Can't calculate MD5 checksum, IOException: ", e);
                        continue;
                    }
                    break;
            }

            LOGGER.debug("New checksum: {}", newChecksum != null ? newChecksum.toString() : "null");

            if (newChecksum != null && newChecksum.equals(checksum)) {
                LOGGER.debug("Checksums are equal: {} ; {}", checksum, newChecksum);
                return newChecksum;
            }
        }
        return null;
    }

    private void stageFinished(StageRequest request, File file) {
        Optional<Set<Checksum>> requestChecksum = request.getFileAttributes().getChecksumsIfPresent();
        String pnfsid = request.getFileAttributes().getPnfsId().toString();

        if (requestChecksum.isPresent()) {
            Set<Checksum> requestChecksums = requestChecksum.get();
            Checksum newChecksum;
            try {
                newChecksum = compareAndReturnChecksum(requestChecksums, file);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            if (newChecksum != null) {
                request.completed(Collections.singleton(newChecksum));
                stageFiles.deleteOne(new Document("pnfsid", pnfsid));
                LOGGER.info("Stage for file {} finished successfully", pnfsid);
            } else {
                LOGGER.error("No checksum could be calculated or matched the original checksum. Failing request!");
                stageFiles.deleteOne(new Document("pnfsid", pnfsid));
                request.failed(44, "Calculating checksum failed or checksums mismatched");
            }
        } else {
            LOGGER.warn("There is no Checksum for file {} in dCache. " +
                    "File is staged without checksum comparison!", pnfsid);
            Set<Checksum> checksums = new HashSet<>();
            try {
                checksums.add(calculateAdler32(file));
            } catch (IOException e) {
                LOGGER.error("Could not calculate Adler32 Checksum of file {} due to IOException: {}",
                        file.getPath(), e);
            }
            try {
                checksums.add(calculateMd5(file));
            } catch (FileNotFoundException e) {
                request.failed(new RuntimeException(e));
            } catch (NoSuchAlgorithmException e) {
                LOGGER.error("Can't calculate MD5 checksum, no algorithm for MD5 found");
            } catch (IOException e) {
                LOGGER.error("Can't calculate MD5 checksum, IOException: ", e);
            }
            request.completed(checksums);
        }
    }

    private void newStageRequest(StageRequest request) {
        LOGGER.debug("Add MongoDB Record");
        String pnfsid = request.getFileAttributes().getPnfsId().toString();
        FindIterable<Document> results = stageFiles.find(new Document("pnfsid", pnfsid));
        try {
            if (results.first() == null) {
                LOGGER.debug("MongoDB record for file {} doesn't exist, will be created now", pnfsid);
                List<URI> locations = request.getFileAttributes().getStorageInfo().locations();
                List<String> locationList = locations.stream().map(URI::toString).collect(Collectors.toList());
                List<BsonString> bsonLocations = locationList.stream().map(BsonString::new).collect(Collectors.toList());
                LOGGER.debug("Locations for file {}: {}", pnfsid, locations);

                Document record = new Document();
                record.append("pnfsid", pnfsid)
                        .append("filepath", request.getReplicaUri().getPath())
                        .append("locations", new BsonArray(bsonLocations))
                        .append("status", "new")
                        .append("driver_url", "https://" + hostname + ":" + port);

                stageFiles.insertOne(record);
            } else {
                LOGGER.debug("MongoDB Record exists already");
            }
            request.allocate();
            LOGGER.info("Start staging process for file {}", pnfsid);
        } catch (MongoException e) {
            LOGGER.error("Record for file {} could not be written to MongoDB", pnfsid, e);
        }

    }

    private void processStage() {
        if (cancelRequest)
        {
            return;
        }
        stageQueueLocked = true;
        LOGGER.debug("processStage() called");
        Queue<StageRequest> notYetReady = new ArrayDeque<>();
        StageRequest request;
        String pnfsid;
        File file;

        while ((request = stageRequestQueue.poll()) != null) {
            pnfsid = request.getFileAttributes().getPnfsId().toString();
            file = new File(request.getReplicaUri());
            Document result;
            LOGGER.debug("Found request for file {} {}", pnfsid, file.getPath());
            try {
                    FindIterable<Document> results = stageFiles.find(new Document("pnfsid", pnfsid));
                    result = results.first();
            } catch (MongoException e) {
                LOGGER.error("An error occured while requesting MongoDB to find files to be staged: ", e);
                notYetReady.add(request);
                continue;
            }

            if (result != null && result.get("status").equals("done")) {
                if (file.exists()) {
                    LOGGER.debug("File {} exists", request.getFileAttributes().getPnfsId());
                    stageFinished(request, file);
                } else {
                    LOGGER.warn("File {} should be uploaded to dCache but was not found. Resetting MongoDB to " +
                            "stage file again", pnfsid);
                    resetFile(pnfsid, file);
                }
            } else if (result != null && result.get("status").equals("failure")) {
                LOGGER.error("Staging the file failed on packer side. Please look into logs of stage-files.py on the packing node for more information!");
                stageFiles.deleteOne(new Document("pnfsid", pnfsid));
                request.failed(new FileNotFoundException("Unable to stage file"));
            } else {
                LOGGER.debug("File not found {}", pnfsid);

                if (result == null) {
                    newStageRequest(request);
                } else {
                    LOGGER.debug("MongoDB record exists");
                }
                notYetReady.add(request);
            }
        }
        LOGGER.debug("NotYetReady size: {} will be added to stageRequestQueue now", notYetReady.size());
        stageRequestQueue.addAll(notYetReady);
        stageQueueLocked = false;

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
        cancelRequest = true;
        while (stageQueueLocked || flushQueueLocked)
        {
            LOGGER.debug("stageQueueLocked = {};; flushQueueLocked = {}", stageQueueLocked, flushQueueLocked);
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                //do nothing, continue while-loop?
            }
        }
        LOGGER.debug("Cancel triggered for UUID {}", uuid);
        Predicate<FlushRequest> flushByUUID = request -> request.getId().equals(uuid);
        Predicate<StageRequest> stageByUUID = request -> request.getId().equals(uuid);
//        Predicate<NearlineRequest> requestByUUID = request -> request.getId().equals(uuid);

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

        cancelRequest = false;
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
        LOGGER.debug("Triggered shutdown()");
        if (mongoClient != null) {
            mongoClient.close();
        }
        executorService.shutdown();
        try {
            if (server != null) {
                server.stopServer();
            }
        } catch (Exception e) {
            LOGGER.error("Error stopping Jetty server", e);
        }
    }

    private void processFlush() {
        if (cancelRequest)
        {
            return;
        }
        flushQueueLocked = true;
        LOGGER.debug("processFlush() called");
        Queue<FlushRequest> notYetReady = new ArrayDeque<>();
        FlushRequest request;

        while ((request = flushRequestQueue.poll()) != null) {

            String pnfsid = request.getFileAttributes().getPnfsId().toString();
            LOGGER.debug("PNFSID: {}", pnfsid);

            if (request.getFileAttributes().getSize() == 0) {
                LOGGER.debug("Filesize is 0");
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
                LOGGER.debug("Result: {}", result.toJson());
                if (result.containsKey("archiveUrl")) {
                    try {
                        files.deleteOne(new Document("pnfsid", pnfsid));
                        String archiveUrl = (String) result.get("archiveUrl");
                        URI fileUri = new URI(archiveUrl.replace("dcache://dcache", type + "://" + name));
                        LOGGER.debug("archiveUrl exists, fileUri: {}", fileUri);
                        LOGGER.info("Location for file {} is {}", pnfsid, fileUri);
                        request.completed(Collections.singleton(fileUri));
                    } catch (URISyntaxException e) {
                        LOGGER.error("Error completing flushRequest: " + e);
                        request.failed(e);
                    }
                } else {
                    notYetReady.offer(request);
                }
            } else {
                Path path = Path.of(request.getFileAttributes().getStorageInfo().getKey("path"));
                LOGGER.debug("Path: " + path);
                String parent;
                try {
                    parent = path.getParent().toString();
                } catch (NullPointerException e) {
                    parent = path.toString();
                }
                Document entry = new Document("pnfsid", pnfsid)
                        .append("store", request.getFileAttributes().getStorageInfo().getKey("store"))
                        .append("group", request.getFileAttributes().getStorageInfo().getKey("group"))
                        .append("path", path.toString())
                        .append("parent", parent)
                        .append("replica_uri", request.getReplicaUri().getPath())
                        .append("size", request.getFileAttributes().getSize())
                        .append("ctime", Double.parseDouble(Long.toString(request.getReplicaCreationTime())) / 1000)
                        .append("hsm_type", this.type)
                        .append("hsm_name", this.name)
                        .append("state", "new")
                        .append("driver_url", "https://" + hostname + ":" + port);
                LOGGER.debug("Inserting to database: {}", entry.toJson());
                files.insertOne(entry);
                notYetReady.offer(request);
            }
        }
        LOGGER.debug("NotYetReady size: {} will be added to flushRequestQueue now", notYetReady.size());
        flushRequestQueue.addAll(notYetReady);
        flushQueueLocked = false;
    }

    private void uncaughtException(Thread t, Throwable e) {
        LOGGER.error("Uncaught exception in {}", t.getName(), e);
    }
}

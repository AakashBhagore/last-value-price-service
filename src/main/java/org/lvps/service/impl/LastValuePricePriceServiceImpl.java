package org.lvps.service.impl;

import org.lvps.model.PriceRecord;
import org.lvps.service.LastValuePriceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Simplified implementation retaining correctness and explainability.
 *
 * S3-style multipart upload analogy:
 *  - startBatch() ~= initiateMultipartUpload()
 *  - uploadChunk() ~= uploadPart()
 *  - completeBatch() ~= completeMultipartUpload()
 *  - cancelBatch()   ~= abortMultipartUpload()
 *  - deleteInstrument() ~= deleteObject()
 *
 * Staged parts are held in-memory until completeBatch() atomically publishes everything.
 */
public class LastValuePricePriceServiceImpl implements LastValuePriceService {

    private static final Logger logger = LoggerFactory.getLogger(LastValuePricePriceServiceImpl.class);

    private final ConcurrentMap<String, ConcurrentSkipListMap<Instant, PriceRecord>> masterStore = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<String, ConcurrentSkipListMap<Instant, PriceRecord>>> stagingBatches = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, BatchState> batchStates = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Map<String, ConcurrentSkipListMap<Instant, PriceRecord>>> snapshots = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private enum BatchState { STARTED, COMPLETED, CANCELLED }

    /**
     * Starts a new batch process and returns a unique batch identifier.
     * This is analogous to initiating a multipart upload in S3.
     *
     * @return A unique string identifying the new batch.
     */
    @Override
    public String startBatch() {
        var batchId = UUID.randomUUID().toString();
        stagingBatches.putIfAbsent(batchId, new ConcurrentHashMap<>());
        batchStates.put(batchId, BatchState.STARTED);
        logger.info("[Producer] Started batch {}", batchId);
        return batchId;
    }

    /**
     * Uploads a chunk of price records to the specified batch.
     * Records are staged and are not visible until the batch is completed.
     * This is analogous to uploading a part in S3.
     *
     * @param batchId The identifier of the batch to upload to.
     * @param chunk   A list of {@link PriceRecord}s to upload.
     * @throws IllegalStateException if the batch is not in the STARTED state.
     */
    @Override
    public void uploadChunk(String batchId, List<PriceRecord> chunk) {
        if (chunk == null || chunk.isEmpty()) return;

        BatchState state = batchStates.get(batchId);
        if (state != BatchState.STARTED) {
            if (state == null) {
                throw new IllegalStateException("Batch not found: " + batchId + ". A new batch must be started before uploading chunks.");
            }
            throw new IllegalStateException("Cannot upload to batch '" + batchId + "' with status " + state);
        }

        // A started batch must have a staging area.
        ConcurrentMap<String, ConcurrentSkipListMap<Instant, PriceRecord>> batchMap = stagingBatches.get(batchId);
        if (Objects.isNull(batchMap)) {
            throw new IllegalStateException("Internal error: Staging area not found for STARTED batch '" + batchId + "'.");
        }

        for (PriceRecord rec : chunk) {
            batchMap.computeIfAbsent(rec.instrumentId(), k -> new ConcurrentSkipListMap<>()).put(rec.asOf(), rec);
        }
        logger.info("[Producer] Uploaded chunk (size={}) for batch {}", chunk.size(), batchId);
    }

    /**
     * Completes the batch, making all uploaded price records atomically visible for querying.
     * This is analogous to completing a multipart upload in S3.
     *
     * @param batchId The identifier of the batch to complete.
     */
    @Override
    public void completeBatch(String batchId) {
        if (!batchStates.replace(batchId, BatchState.STARTED, BatchState.COMPLETED)) {
            BatchState currentState = batchStates.get(batchId);
            if (currentState == BatchState.CANCELLED) {
                logger.warn("[System] Warning: Attempted to complete a cancelled batch {} - ignoring (staged data discarded)", batchId);
                stagingBatches.remove(batchId); // Defensive cleanup
            } else if (currentState != BatchState.COMPLETED) { // Don't log if already completed
                logger.warn("[System] Warning: Attempted to complete a batch {} in an unknown or invalid state: {}", batchId, currentState);
            }
            return;
        }

        final Map<String, ConcurrentSkipListMap<Instant, PriceRecord>> batchMap = stagingBatches.remove(batchId);

        if (batchMap == null || batchMap.isEmpty()) {
            logger.info("[Producer] Completed empty batch {} - nothing to publish", batchId);
            return;
        }

        lock.writeLock().lock();
        try {
            int recordsMerged = 0;
            for (Map.Entry<String, ConcurrentSkipListMap<Instant, PriceRecord>> entry : batchMap.entrySet()) {
                String instrument = entry.getKey();
                ConcurrentSkipListMap<Instant, PriceRecord> batchSeries = entry.getValue();
                if (batchSeries != null && !batchSeries.isEmpty()) {
                    masterStore.computeIfAbsent(instrument, k -> new ConcurrentSkipListMap<>()).putAll(batchSeries);
                    recordsMerged += batchSeries.size();
                }
            }
            logger.info("[System] Success: Batch {} published — {} records now visible", batchId, recordsMerged);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Cancels the batch and discards all its staged price records.
     * This is analogous to aborting a multipart upload in S3.
     *
     * @param batchId The identifier of the batch to cancel.
     */
    @Override
    public void cancelBatch(String batchId) {
        if (batchStates.replace(batchId, BatchState.STARTED, BatchState.CANCELLED)) {
            stagingBatches.remove(batchId);
            logger.info("[Producer] Success: Cancelled batch {}", batchId);
        } else {
            BatchState currentState = batchStates.get(batchId);
            if (currentState != BatchState.CANCELLED && currentState != BatchState.COMPLETED) {
                logger.warn("[Producer] Could not cancel batch {}. Current state: {}", batchId, currentState);
            }
        }
    }

    /**
     * Retrieves the latest price for a given instrument at or before the specified query time.
     *
     * @param instrumentId The identifier of the instrument.
     * @param queryTime    The time to query at.
     * @return An {@link Optional} containing the {@link PriceRecord} if found, otherwise empty.
     */
    @Override
    public Optional<PriceRecord> getLastPriceAt(String instrumentId, Instant queryTime) {
        if (instrumentId == null || queryTime == null) return Optional.empty();
        lock.readLock().lock();
        try {
            ConcurrentSkipListMap<Instant, PriceRecord> series = masterStore.get(instrumentId);
            if (series == null) return Optional.empty();
            Map.Entry<Instant, PriceRecord> priceEntry = series.floorEntry(queryTime);
            return Optional.ofNullable(priceEntry).map(Map.Entry::getValue);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void deleteInstrument(String instrumentId) {
        lock.writeLock().lock();
        try {
            masterStore.remove(instrumentId);
            logger.info("[System] Deleted instrument {} from master store", instrumentId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public java.util.List<org.lvps.model.PriceRecord> getHistory(String instrumentId) {
        lock.readLock().lock();
        try {
            ConcurrentSkipListMap<java.time.Instant, PriceRecord> series = masterStore.get(instrumentId);
            if (series == null) return java.util.Collections.emptyList();
            return new java.util.ArrayList<>(series.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String createReadSnapshot() {
        String snapshotId = UUID.randomUUID().toString();
        lock.readLock().lock();
        try {
            Map<String, ConcurrentSkipListMap<Instant, PriceRecord>> snapshotData = new HashMap<>();
            for (Map.Entry<String, ConcurrentSkipListMap<Instant, PriceRecord>> entry : masterStore.entrySet()) {
                snapshotData.put(entry.getKey(), new ConcurrentSkipListMap<>(entry.getValue()));
            }
            snapshots.put(snapshotId, snapshotData);
            logger.info("[System] Created read snapshot {} with {} instruments", snapshotId, snapshotData.size());
            return snapshotId;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<PriceRecord> getLastPriceAtSnapshot(String snapshotId, String instrumentId, Instant queryTime) {
        if (snapshotId == null || instrumentId == null || queryTime == null) return Optional.empty();
        Map<String, ConcurrentSkipListMap<Instant, PriceRecord>> snapshot = snapshots.get(snapshotId);
        if (snapshot == null) {
            throw new IllegalArgumentException("Snapshot not found: " + snapshotId);
        }
        ConcurrentSkipListMap<Instant, PriceRecord> series = snapshot.get(instrumentId);
        if (series == null) return Optional.empty();
        Map.Entry<Instant, PriceRecord> priceEntry = series.floorEntry(queryTime);
        return Optional.ofNullable(priceEntry).map(Map.Entry::getValue);
    }

    @Override
    public void releaseSnapshot(String snapshotId) {
        Map<String, ConcurrentSkipListMap<Instant, PriceRecord>> removed = snapshots.remove(snapshotId);
        if (removed != null) {
            logger.info("[System] Released snapshot {} containing {} instruments", snapshotId, removed.size());
        }
    }
}

package org.lvps.service;

import org.lvps.model.PriceRecord;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * LastValuePriceService
 *
 * Defines the contract for ingesting time-ordered price records for financial instruments and
 * querying the latest known price at or before a given timestamp ("last value price").
 *
 * Requirements and behavior
 * - Batch-oriented ingestion: Data is uploaded in batches to ensure atomicity and efficiency.
 *   - startBatch(): starts a new batch session and returns a batchId used for subsequent calls.
 *   - uploadChunk(batchId, chunk): one or more chunks can be uploaded under an open batch. Each
 *     chunk contains zero or more PriceRecord entries for possibly multiple instruments. The
 *     implementation must tolerate repeated calls and chunk sizes of varying length. Chunks within
 *     the same batch may arrive out of order; ordering is derived from PriceRecord timestamps.
 *   - completeBatch(batchId): atomically commits the uploaded batch contents, making them available
 *     for queries. If a batch is never completed, its data must not become visible to queries.
 *   - cancelBatch(batchId): discards any uploaded data for the batch and releases resources.
 *
 * - Idempotency and deduplication: If the same PriceRecord (instrumentId + timestamp) is uploaded
 *   multiple times in the same or different batches, the most recent value for the same key should
 *   overwrite the previous one per implementation policy. Implementations should avoid duplicating
 *   storage for identical records.
 *
 * - Query semantics: getLastPriceAt(instrumentId, queryTime) returns the latest PriceRecord whose
 *   timestamp is less than or equal to queryTime. Returns Optional.empty() when no such record
 *   exists (no history or only records after queryTime).
 *
 * - Concurrency: Reads and writes may occur concurrently. Implementations should ensure that
 *   committed batches are visible atomically and that in-flight batches do not leak partial state.
 *
 * - Data retention and management: deleteInstrument(instrumentId) removes all records for the
 *   specified instrument, and getHistory(instrumentId) returns the full chronological history that
 *   the implementation stores (ordering ascending by timestamp is recommended).
 *
 * Thread-safety and performance considerations are implementation-specific but recommended:
 * - Use time-ordered structures for fast last-value lookups (e.g., tree maps or skip lists).
 * - Prefer immutable snapshots or copy-on-write mechanics to provide atomic visibility on commit.
 * - Validate input: null checks, monotonic timestamps within a record, and consistent batch states.
 */
public interface LastValuePriceService {
    /**
     * Begin a new ingestion batch.
     * @return generated batch identifier to be used with subsequent calls
     */
    String startBatch();

    /**
     * Upload a chunk of price records under an open batch. Chunks may arrive out of order.
     * @param batchId active batch identifier
     * @param chunk list of PriceRecord entries (can be empty)
     */
    void uploadChunk(String batchId, List<PriceRecord> chunk);

    /**
     * Atomically commit the batch, making its records visible to queries.
     * @param batchId active batch identifier
     */
    void completeBatch(String batchId);

    /**
     * Cancel the batch and discard any uploaded records.
     * @param batchId active batch identifier
     */
    void cancelBatch(String batchId);

    /**
     * Get the latest price at or before the specified time for an instrument.
     * @param instrumentId instrument identifier
     * @param queryTime timestamp cutoff (inclusive)
     * @return Optional containing the last PriceRecord or empty if none
     */
    Optional<PriceRecord> getLastPriceAt(String instrumentId, Instant queryTime);

    // Management operations
    /**
     * Delete all stored history for the given instrument.
     * @param instrumentId instrument identifier
     */
    void deleteInstrument(String instrumentId);

    /**
     * Retrieve full stored history for the instrument.
     * @param instrumentId instrument identifier
     * @return list of PriceRecord entries (implementation-defined order)
     */
    List<PriceRecord> getHistory(String instrumentId);

    // Read Snapshot operations
    /**
     * Create a consistent read snapshot of the current data state.
     * @return snapshot identifier for subsequent queries
     */
    String createReadSnapshot();

    /**
     * Get the latest price at or before the specified time using a read snapshot.
     * @param snapshotId snapshot identifier
     * @param instrumentId instrument identifier
     * @param queryTime timestamp cutoff (inclusive)
     * @return Optional containing the last PriceRecord or empty if none
     */
    Optional<PriceRecord> getLastPriceAtSnapshot(String snapshotId, String instrumentId, Instant queryTime);

    /**
     * Release resources associated with the read snapshot.
     * @param snapshotId snapshot identifier
     */
    void releaseSnapshot(String snapshotId);
}

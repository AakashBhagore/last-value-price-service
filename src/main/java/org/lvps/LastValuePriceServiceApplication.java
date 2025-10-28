package org.lvps;

import org.lvps.model.PriceRecord;
import org.lvps.service.LastValuePriceService;
import org.lvps.service.impl.LastValuePricePriceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * LastValuePriceServiceApplication - produces 1000 instrument prices in chunks and then consumers query after publish.
 */
public class LastValuePriceServiceApplication {

    private static final Logger logger = LoggerFactory.getLogger(LastValuePriceServiceApplication.class);
    private static final int INSTRUMENT_COUNT = 1000;
    private static final int CHUNK_SIZE = 100;
    private static final int CONSUMER_THREADS = 4;
    private static final int CONSUMER_QUERIES = 30;

    public static void main(String[] args) throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(CONSUMER_THREADS + 1)) {
            LastValuePriceService service = new LastValuePricePriceServiceImpl();

            // Producer: create 1000 instruments and upload in chunks
            Runnable producer = () -> {
                var batchId = service.startBatch();
                logger.info("[Producer] Starting batch {}", batchId);

                List<PriceRecord> records = new ArrayList<>(INSTRUMENT_COUNT);
                var now = Instant.now();
                DateTimeFormatter fmt = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);
                for (int index = 0; index < INSTRUMENT_COUNT; index++) {
                    var instrument = "INSTR_" + index;
                    var price = String.format(Locale.ROOT, "%.2f", 1000.00 + index);
                    // assign asOf in the past so consumers querying 'now' can see them
                    var asOf = now.minusSeconds(INSTRUMENT_COUNT - index);
                    records.add(new PriceRecord(instrument, asOf, price));
                    if (index < 5 || index % 250 == 0) {
                        logger.info("[Producer] Producing {} -> {} (asOf={})", instrument, price, fmt.format(asOf));
                    }
                }

                // upload in chunks
                var chunkNo = 0;
                for (int start = 0; start < records.size(); start += CHUNK_SIZE) {
                    var end = Math.min(records.size(), start + CHUNK_SIZE);
                    List<PriceRecord> chunk = records.subList(start, end);
                    chunkNo++;
                    service.uploadChunk(batchId, chunk);
                    logger.info("[Producer] Uploaded chunk {} of {} (size={})", chunkNo, (records.size() + CHUNK_SIZE - 1) / CHUNK_SIZE, chunk.size());
                }

                // complete and publish atomically
                service.completeBatch(batchId);
                logger.info("[Producer] Completed batch {}", batchId);
            };

            // Start producer
            Future<?> pf = executor.submit(producer);
            // Wait for producer to finish before starting consumers
            pf.get(60, TimeUnit.SECONDS);

            // Consumers start after publish (per your preference)
            Callable<Void> consumerTask = () -> {
                Random rnd = new Random();
                String consumerSnapshot = service.createReadSnapshot();
                try {
                    for (int index = 0; index < CONSUMER_QUERIES; index++) {
                        int idx = rnd.nextInt(INSTRUMENT_COUNT);
                        String instrument = "INSTR_" + idx;
                        Optional<PriceRecord> p = service.getLastPriceAtSnapshot(consumerSnapshot, instrument, Instant.now());
                        if (index % 5 == 0) {
                            logger.info("[Consumer-{}] Snapshot query {} -> {}", Thread.currentThread().getName(), instrument, p.map(PriceRecord::payload).orElse("N/A"));
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ignored) {
                        }
                    }
                } finally {
                    service.releaseSnapshot(consumerSnapshot);
                }
                return null;
            };

            List<Future<Void>> consumers = new ArrayList<>();
            for (int index = 0; index < CONSUMER_THREADS; index++) {
                consumers.add(executor.submit(consumerTask));
            }

            for (Future<Void> future : consumers) {
                try {
                    future.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.warn("Consumer task error", e);
                }
            }

            // Final snapshot for a few instruments
            logger.info("=== FINAL SNAPSHOT ===");
            for (int index = 0; index < 10; index++) {
                String instr = "INSTR_" + index;
                service.getLastPriceAt(instr, Instant.now()).ifPresent(p -> logger.info("[Final] {} -> {}", instr, p.payload()));
            }

            // Demonstrate Read Snapshot functionality
            logger.info("=== READ SNAPSHOT DEMO ===");
            String snapshotId = service.createReadSnapshot();
            
            // Query using snapshot for consistent reads
            for (int i = 0; i < 3; i++) {
                String instr = "INSTR_" + (i * 100);
                Optional<PriceRecord> snapshotResult = service.getLastPriceAtSnapshot(snapshotId, instr, Instant.now());
                snapshotResult.ifPresent(p -> logger.info("[Snapshot] {} -> {}", instr, p.payload()));
            }
            
            service.releaseSnapshot(snapshotId);

            // Demonstrate management ops: history & delete (non-critical)
            logger.info("=== Management: getHistory(INSTR_49) size={} ===", service.getHistory("INSTR_49").size());
            service.deleteInstrument("INSTR_999");
            logger.info("Deleted INSTR_999; subsequent getLastPriceAt returns {}", service.getLastPriceAt("INSTR_999", Instant.now()).orElse(null));

            executor.shutdownNow();
            logger.info("=== DEMO COMPLETE (with Read Snapshots) ===");
        } catch (InterruptedException exception) {
            throw new InterruptedException(exception.getLocalizedMessage());
        }
    }
}

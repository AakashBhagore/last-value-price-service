# Last Value Price Service — Demo (Java 21, SLF4J)

## Overview
This project demonstrates an in-memory Last Value Price Service that supports batch uploads and atomic publishing semantics.
The demo produces 1000 dummy instrument price records (INSTR_0..INSTR_999), uploads them in 10 chunks, completes the batch and then consumers query prices.

## How to run
Prerequisites: Java 21, Maven 3.8+

Unzip the project and run:
```
mvn clean compile exec:java -Dexec.mainClass=org.lvps.LastValuePriceServiceApplication
```

Run tests:
```
mvn test
```

## Cancel / Abort Behavior
Producers may cancel batches using `cancelBatch(batchId)` which discards staged data and ensures it is never published.

## S3 multipart upload analogy
This service closely mirrors S3 multipart upload concepts: startBatch -> uploadChunk -> completeBatch / cancelBatch.

## Read Snapshot Feature
- `createReadSnapshot()` creates a consistent point-in-time view of all data
- `getLastPriceAtSnapshot(snapshotId, instrumentId, queryTime)` queries using snapshot for consistent reads
- `releaseSnapshot(snapshotId)` cleans up snapshot resources

## Management additions
- `deleteInstrument(String instrumentId)` removes an instrument from the master store.
- `getHistory(String instrumentId)` returns full history for debugging or demo verification.

## Expected logs (SLF4J simple)
INFO [Producer] Started batch ...
INFO [Producer] Uploaded chunk (size=100) ...
INFO [System] Success Batch ... published — 1000 records now visible
INFO [System] Created read snapshot ... with 1000 instruments
INFO [Consumer] Snapshot query INSTR_49 -> 1049.00
INFO [Snapshot] INSTR_0 -> 1000.00
INFO [System] Released snapshot ... containing 1000 instruments

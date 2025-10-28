package org.lvps.service.impl;

import org.junit.jupiter.api.Test;
import org.lvps.model.PriceRecord;
import org.lvps.service.LastValuePriceService;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class LastValuePriceServiceSnapshotTest {

    @Test
    void readSnapshot_providesConsistentView() {
        LastValuePriceService service = new LastValuePricePriceServiceImpl();
        
        // Initial batch
        String batch1 = service.startBatch();
        service.uploadChunk(batch1, List.of(
            new PriceRecord("SNAP_TEST", Instant.now().minusSeconds(10), "100.00")
        ));
        service.completeBatch(batch1);
        
        // Create snapshot
        String snapshotId = service.createReadSnapshot();
        
        // Add more data after snapshot
        String batch2 = service.startBatch();
        service.uploadChunk(batch2, List.of(
            new PriceRecord("SNAP_TEST", Instant.now(), "200.00")
        ));
        service.completeBatch(batch2);
        
        // Snapshot should show old data, current query shows new data
        Optional<PriceRecord> snapshotResult = service.getLastPriceAtSnapshot(snapshotId, "SNAP_TEST", Instant.now());
        Optional<PriceRecord> currentResult = service.getLastPriceAt("SNAP_TEST", Instant.now());
        
        assertTrue(snapshotResult.isPresent());
        assertTrue(currentResult.isPresent());
        assertEquals("100.00", snapshotResult.get().payload());
        assertEquals("200.00", currentResult.get().payload());
        
        service.releaseSnapshot(snapshotId);
    }
    
    @Test
    void releaseSnapshot_cleansUpResources() {
        LastValuePriceService service = new LastValuePricePriceServiceImpl();
        
        String batch = service.startBatch();
        service.uploadChunk(batch, List.of(
            new PriceRecord("CLEANUP_TEST", Instant.now(), "50.00")
        ));
        service.completeBatch(batch);
        
        String snapshotId = service.createReadSnapshot();
        service.releaseSnapshot(snapshotId);
        
        // Using released snapshot should throw exception
        assertThrows(IllegalArgumentException.class, () -> 
            service.getLastPriceAtSnapshot(snapshotId, "CLEANUP_TEST", Instant.now())
        );
    }
}
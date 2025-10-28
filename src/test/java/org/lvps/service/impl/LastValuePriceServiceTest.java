package org.lvps.service.impl;

import org.junit.jupiter.api.Test;
import org.lvps.model.PriceRecord;
import org.lvps.service.LastValuePriceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class LastValuePriceServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(LastValuePriceServiceTest.class);

    @Test
    void demoProducerConsumerFlow() {
        LastValuePriceService service = new LastValuePricePriceServiceImpl();
        String batchId = service.startBatch();
        logger.info("[Producer] Started batch {}", batchId);

        List<PriceRecord> records = new ArrayList<>();
        Instant now = Instant.now();
        for (int i = 0; i < 1000; i++) {
            records.add(new PriceRecord("INSTR_" + i, now.minusSeconds(i), String.valueOf(1000.0 + i)));
        }

        service.uploadChunk(batchId, records);
        logger.info("[Producer] Uploaded chunk (size={}) for batch {}", records.size(), batchId);

        service.completeBatch(batchId);
        logger.info("[System] Success Batch {} published — {} records now visible", batchId, records.size());

        String[] sampleIds = {"INSTR_0", "INSTR_49", "INSTR_999"};
        for (String id : sampleIds) {
            Optional<PriceRecord> record = service.getLastPriceAt(id, Instant.now());
            assertTrue(record.isPresent(), "Expected visible record for " + id);
            logger.info("[Consumer] Last known value for {} is {}", id, record.get().payload());
        }

        Optional<PriceRecord> check = service.getLastPriceAt("INSTR_49", Instant.now());
        assertTrue(check.isPresent());
        assertEquals("1049.0", check.get().payload());
        logger.info("[Verification] Success Last read value for INSTR_49 is {}", check.get().payload());
    }
}

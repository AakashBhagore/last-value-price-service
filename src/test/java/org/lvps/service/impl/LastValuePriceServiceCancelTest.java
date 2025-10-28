package org.lvps.service.impl;

import org.junit.jupiter.api.Test;
import org.lvps.model.PriceRecord;
import org.lvps.service.LastValuePriceService;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class LastValuePriceServiceCancelTest {

    @Test
    void cancelBatch_discardStagedData() {
        LastValuePriceService svc = new LastValuePricePriceServiceImpl();
        String batch = svc.startBatch();
        svc.uploadChunk(batch, List.of(new PriceRecord("CANCEL_TEST", Instant.now(), "500")));
        svc.cancelBatch(batch);
        svc.completeBatch(batch);
        Optional<PriceRecord> p = svc.getLastPriceAt("CANCEL_TEST", Instant.now());
        assertTrue(p.isEmpty(), "Cancelled batch data should never be visible");
    }
}

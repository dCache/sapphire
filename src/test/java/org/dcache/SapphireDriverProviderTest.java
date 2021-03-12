package org.dcache;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SapphireDriverProviderTest {

    @Test
    public void testProviderName() {
        SapphireDriverProvider provider = new SapphireDriverProvider();
        assertEquals("sapphire", provider.getName(), "the provider name is incorrect");
    }

    @Test
    public void testDescriptionNotNull() {
        SapphireDriverProvider provider = new SapphireDriverProvider();
        assertNotNull(provider.getDescription(), "Description can be null");
    }

    @Test
    public void testDriverType() {
        SapphireDriverProvider provider = new SapphireDriverProvider();

        NearlineStorage nearlineStorage = provider.createNearlineStorage("foo", "bar");
        try {
            assertEquals(SapphireDriver.class, nearlineStorage.getClass(), "incorrect driver type");
        } finally {
            nearlineStorage.shutdown();
        }
    }

}

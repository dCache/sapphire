package org.dcache;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class PluginNearlineStorageProvider implements NearlineStorageProvider
{
    @Override
    public String getName()
    {
        return "SmallFiles-Driver";
    }

    @Override
    public String getDescription()
    {
        return "Plugin that replaces hsm_internal.sh from Small Files Plugin";
    }

    @Override
    public NearlineStorage createNearlineStorage(String type, String name)
    {
        return new SmallFilesDriver(type, name);
    }
}

Sapphire Plugin - Java driver for dCache
==================================

This is the driver part of Sapphire.

To compile the plugin, run:

    mvn package

This produces a tarball in the `target` directory containing the plugin.

Using the plugin with dCache
----------------------------

To use this plugin with dCache, place the directory containing this
file in /usr/local/share/dcache/plugins/ on a dCache pool. Restart
the pool to load the plugin.

To verify that the plugin is loaded, navigate to the pool in the dCache admin
shell and issue the command:

    hsm show providers

The plugin should be listed as *Sapphire*.

To activate the plugin, create an HSM instance using:

    hsm create osm name Sapphire [-key=value]...


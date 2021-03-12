Nearline Storage Plugin for dCache
==================================

This is nearline storage plugin for dCache.

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

The plugin should be listed.

To activate the plugin, create an HSM instance using:

    hsm create osm name Sapphire [-key=value]...

The available configuration options:

| Name | Description | required | default |
| :--- | :--- | ---: | --- |
database | The mongo database name | yes | -
mongo_url | The mongodb connection url | yes | -
period | The period between successive scans of flush queue | no | 1
period_unit | The the time unit of period, SECONDS, MINUTES ... | no | MINUTES
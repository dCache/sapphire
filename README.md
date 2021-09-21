Sapphire Plugin for dCache
==================================

1. What is Sapphire?
--------------------
Sapphire is a dCache-plugin for packing small files into bigger ones intended
to improve performance in writing files to tape. Sapphire is divided into
two parts: driver and packer. The driver is used directly in dCache while the packer
runs separately, usually on a dedicated machine.

**Requirements**
- Driver:
    - dCache 6.2.19/7.0.7/7.1.0 or higher with
        - WebDav
        - Frontend
- Packer:
    - Python 3
        - pymongo 3.11.0 or higher
        - requests
    - MongoDB 4.4 or higher

2. Preparations
----------------
General:

Sapphire needs a MongoDB to run correctly. For installation and configuration take a look
into the MongoDB documentation. The MongoDB has to be accessible for all machines that
are parts of Sapphire. Inside of MongoDB a database is needed. The name can be chosen
freely in compliance with the rules MongoDB has itself for naming databases. The database
has two collections that need to be configured further, other collections are created
with the scripts themselves. Run the following commands via Mongoshell:

```
db.files.ensureIndex( { ctime: 1 }, { sparse: true } )
db.files.ensureIndex( { pnfsid: 1}, { dropDups: true, unique: true } )
db.stage.ensureIndex( { pnfsid: 1}, { dropDups: true, unique: true } )
```

Driver and dCache:

On the driver side, dCache has to be prepared to interact with a tertiary storage system.
Follow the link to find instructions on how to configure pool(s) to run correctly:
https://dcache.org/old/manuals/Book-5.0/config-hsm.shtml#configuring-pools-to-interact-with-a-tertiary-storage-system.

To get the plugin for dCache, compile the plugin with running `mvn package` in the folder
`driver` from this repository. This will create a new directory, called `target`,
where a tarball can be found that is needed later in Step 3, "Installation and start".

Packer:

There's a configuration file that has to be filled: container.conf. The file is located
in `/etc/dcache/container.conf` by default but can be placed somewhere else and be renamed,
too. The single parameters in this file are explained in itself. It has a `DEFAULT` section
on the top that is used by the packer-part. Below this `DEFAULT` section
is space to create further sections which are needed for the packing itself. With these
sections it's possible to define rules for different directories. The names of the sections
can be chosen freely.  Please read the chapter about Macaroons in dCache-UserGuide to
learn how to get one for the configuration. In the end create a directory
`/var/log/dcache`.

3. Installation and start
-------------------------
Packer:

To install the packer simply move the three python scripts, `pack-files.py`,
`verify-container.py` and `stage-files.py`, to `/usr/local/bin`. Give them the
permission to be executed with `chmod +x` and run them afterwards. To run the
scripts in background, the following commands can be used as root:
```
nohup /usr/local/bin/pack-files.py > /tmp/pack-files.log &
nohup /usr/local/bin/verify-container.py > /tmp/verify-container.log &
nohup /usr/local/bin/stage-files.py > /tmp/stage-files.log &
```
If the configuration file is not `/etc/dcache/container.conf`, the correct path with the
filename has to be given as a parameter to the scripts.

Driver:

Take the .tar-gz-file of the plugin and unpack it into
`/usr/local/share/dcache/plugins`. If this directory doesn't exist, simply create
it. Run `systemctl daemon-reload` and restart the pool where the plugin
should run. In the admin shell, connected to the pool, the plugin
should appear after running `hsm show providers`. If this is the case, create
an instance of the plugin with

    hsm create <instance> <name> sapphire [-key=value]...

Please make sure, `<instance>` matches the tag `hsmInstance` of the directory that
contains the files to be packed.

The available configuration options:

| Name | Description | required | default |
| :--- | :--- | ---: | --- |
database | The mongo database name | yes | -
mongo_url | The mongodb connection url | yes | -
port | The port where the plugin should run | yes | -
whitelist | Whitelist TODO | yes or no| -
period | The period between successive scans of flush queue | no | 1
period_unit | The time unit of period, SECONDS, MINUTES ... | no | MINUTES
certfile | The path to the certificate file for TLS | no | `/etc/dcache/grid-security/hostcert.pem`
keyfile | The path to the key file for TLS | no | `/etc/dcache/grid-security/hostkey.pem`

After successful creation of the hsm instance the packing should work.

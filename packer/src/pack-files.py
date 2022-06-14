#!/usr/bin/env python3
# coding=utf-8

import configparser
import logging
import logging.handlers
import os
import pymongo.errors
import re
import requests
import signal
import sys
import time
import traceback
import uuid
import zipfile

from pymongo import MongoClient, ASCENDING
from datetime import datetime
from zipfile import ZipFile

running = True
mongo_url = ""
working_directory = ""
script_id = ""
loop_delay = -1
logger = logging.getLogger()
mongo_db = None
driver_url = ""
verify = True


def sigint_handler(signum, frame):
    global running
    logging.info(f"Caught signal {signum}.")
    print(f"Caught signal {signum}.")
    running = False


def uncaught_handler(*exc_info):
    err_text = "".join(traceback.format_exception(*exc_info))
    logging.critical(err_text)
    sys.stderr.write(err_text)


def get_config(configfile):
    global mongo_url
    global working_directory
    global script_id
    global loop_delay
    global driver_url
    global verify
    logger.info(f"Reading configuration from file {configfile}")
    configuration = configparser.RawConfigParser(
        defaults={'script_id': 'pack', 'mongo_url': 'mongodb://localhost:27017/', 'mongo_db': 'smallfiles',
                  'loop_delay': 5, 'log_level': 'ERROR', 'working_dir': '/sapphire'})

    try:
        if not os.path.isfile(configfile):
            raise FileNotFoundError
        configuration.read(configfile)

        script_id = configuration.get('DEFAULT', 'script_id')
        log_level_str = configuration.get('DEFAULT', 'log_level')
        mongo_url = configuration.get('DEFAULT', 'mongo_url')
        mongo_db_name = configuration.get('DEFAULT', 'mongo_db')
        working_directory = configuration.get('DEFAULT', 'working_dir')
        loop_delay = configuration.get('DEFAULT', 'loop_delay')
        driver_url = configuration.get('DEFAULT', 'driver_url')
        if configuration.get('DEFAULT', 'verify'):
            verify = configuration.get('DEFAULT', 'verify')
    except FileNotFoundError as e:
        logging.critical(f'Configuration file "{configfile}" not found.')
        raise
    except configparser.NoSectionError as e:
        logging.critical(
            f'Section [DEFAULT] was not found in "{configfile}". This section is mandatory.')
        raise
    except configparser.NoOptionError as e:
        logging.critical(f'An option is missing in section [DEFAULT] of file "{configfile}", exiting now: {e}')
        raise
    except KeyError as e:
        logging.critical(f"There's something wrong with a key, {e}")
        raise
    except configparser.MissingSectionHeaderError as e:
        logging.critical(f'The file "{configfile}" doesn\'t contain section headers. Exiting now')
        raise
    except configparser.ParsingError as e:
        logging.critical(
            f'There was an error parsing while parsing the configuration "{configfile}", exiting now: {e}')
        raise
    except configparser.DuplicateSectionError as e:
        logging.critical(f"There are duplicated sections: {e}")
        raise
    except configparser.DuplicateOptionError as e:
        logging.critical(f"There are duplicated options: {e}")
        raise
    except configparser.Error as e:
        logging.critical(f'An error occurred while reading the configuration file {configfile}, exiting now: {e}')
        raise

    if log_level_str.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        logging.error(f"Log level {log_level_str} is invalid")
        raise ValueError(f"Invalid log_level {log_level_str}. Must be one of (DEBUG|INFO|WARNING|ERROR|CRITICAL)")

    if not os.path.isdir(working_directory):
        logging.info(f"Working directory {working_directory} doesn't exists and will be created now.")
        try:
            os.mkdir(working_directory)
            os.mkdir(f"{working_directory}/container")
        except OSError as e:
            logging.critical(f"Working directory {working_directory} could not be created: {e}")
            raise
        logging.info(f"Working directory was created")

    try:
        loop_delay = int(loop_delay)
    except ValueError as e:
        logging.critical(f"Value of loop delay is invalid: {e}")
        raise

    if any(i in script_id for i in ["/", "$", "\\00"]):
        logging.error("script_id contains chars that are not valid")
        raise ValueError("script_id contains invalid chars like /, $ or \\00")

    if '.' in mongo_db_name:
        logging.error("Invalid database name")
        raise ValueError("mongo_db contains an invalid charakter like '.'")

    try:
        loop_delay = int(loop_delay)
    except ValueError as e:
        logging.critical("The value of loop_delay could not be converted to int")
        raise

    return configuration


class Group:
    def __init__(self, name, configuration):
        self.name = name
        self.configuration = configuration
        self.file_pattern = None
        self.store_group = None
        self.store_name = None
        self.archive_size = None
        self.min_age = None
        self.max_age = None
        self.verify = None
        self.path_regex = None
        self.archive_path = None

        self.read_config()

        logger.debug(f"Load configuration for group {self.name}")
        logger.debug(f"file_pattern: {self.file_pattern}")
        logger.debug(f"store_group: {self.store_group}")
        logger.debug(f"store_name: {self.store_name}")
        logger.debug(f"archive_size: {self.archive_size}")
        logger.debug(f"min_age: {self.min_age}")
        logger.debug(f"max_age: {self.max_age}")
        logger.debug(f"verify: {self.verify}")
        logger.debug(f"path_expression: {self.path_regex}")
        logger.debug(f"archive_path: {self.archive_path}")

        self.create_packager()

    def read_config(self):
        try:
            self.file_pattern = self.configuration.get(self.name, "file_expression")
            self.store_group = self.configuration.get(self.name, "s_group")
            self.store_name = self.configuration.get(self.name, "store_name")
            archive_size = self.configuration.get(self.name, "archive_size")
            self.min_age = self.configuration.get(self.name, "min_age")
            self.max_age = self.configuration.get(self.name, "max_age")
            self.verify = self.configuration.get(self.name, "verify")
            self.path_regex = re.compile(self.configuration.get(self.name, "path_expression"))
            self.archive_path = self.configuration.get(self.name, "archive_path")
        except configparser.NoOptionError as e:
            logger.critical(
                f'An option is missing in section {self.name}", exiting now: {e}')
            raise
        except KeyError as e:
            logger.critical(f"There's something wrong with a key, {e}")
            raise
        except configparser.ParsingError as e:
            logger.critical(
                f'There was an error parsing while parsing the configuration, section {self.name}, '
                f'exiting now: {e}')
            raise
        except configparser.DuplicateOptionError as e:
            logger.critical(f"There are duplicated options: {e}")
            raise
        except configparser.Error as e:
            logger.critical(
                f'An error occurred while reading the configuration, section {self.name}, '
                f'exiting now: {e}')
            raise
        except re.error as e:
            logger.critical(f"An error occured with path_expression in group {self.name}: {e}")
            raise

        try:
            self.archive_size = int(archive_size.replace('G', '000000000').replace('M', '000000')
                                    .replace('K', '000'))
        except ValueError as e:
            logger.critical(f"Value of archive size in section {self.name} is invalid: {e}")
            raise

        if not self.min_age.isnumeric():
            logger.critical(f"The minimum age in section {self.name} is invalid as it's not numerical!")
            raise

        if not self.max_age.isnumeric():
            logger.critical(f"The maximum age in section {self.name} is invalid as it's not numerical!")
            raise

    def create_packager(self):
        try:
            paths = mongo_db.files.find({"parent": self.path_regex}).distinct("parent")
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError) as e:
            logger.warning(f"Could not retrieve paths from MongoDB: {e}")
            return

        pathset = set()
        for path in paths:
            try:
                pathmatch = re.match(f"(?P<sfpath>{self.path_regex.pattern})", path).group("sfpath")
            except re.error as e:
                logger.critical(f"An error occured while matching path {path}: {e}")
                continue
            pathset.add(pathmatch)

        for path in pathset:
            try:
                packager = GroupPackager(path, self.file_pattern, self.store_group, self.store_name, self.archive_size,
                                         self.min_age, self.max_age, self.verify, self.archive_path)
            except re.error as e:
                logger.critical(f"Could not create GroupPackager for path {path}: {e}")
                continue
            logger.info(f"Added packager {self.name} for paths matching {packager.path}")
            packager.run()


class GroupPackager:
    def __init__(self, path, file_pattern, store_group, store_name, archive_size,
                 min_age, max_age, verify, archive_path):
        self.path = path
        self.file_pattern = file_pattern
        self.store_group = store_group
        self.store_name = store_name
        self.archive_size = archive_size
        self.min_age = min_age
        self.max_age = max_age
        self.verify = verify
        self.archive_path = archive_path

        try:
            self.path_pattern = re.compile(os.path.join(path, file_pattern))
            self.logger = logging.getLogger(name=f"GroupPackager[{self.path_pattern.pattern}]")
            self.store_group = re.compile(store_group)
            self.store_name = re.compile(store_name)
        except re.error as e:
            self.logger.critical(f"Error compiling regex while creating GroupPackager: {e}")
            raise

    def write_status(self, arcfile, current_size, next_file):
        global script_id
        with open(f"/var/log/dcache/pack-files-{script_id}.status", 'w') as statusFile:
            statusFile.write(f"Container: {arcfile}\n")
            statusFile.write(f"Size: {current_size}/{self.archive_size}\n")
            statusFile.write(f"Next: {next_file.encode('ascii', 'ignore')}\n")

    def run(self):
        now = int(datetime.now().strftime("%s"))
        ctime_threshold = now - int(self.min_age) * 60
        self.logger.debug(f"Looking for files matching {{ "
                          f"state: new, "
                          f"path: {self.path_pattern.pattern}, "
                          f"group: {self.store_group.pattern}, "
                          f"store: {self.store_name.pattern}, "
                          f"ctime: {{ $lt: {ctime_threshold} }} }}")

        with mongo_db.files.find({
            'state': 'new',
            'path': self.path_pattern,
            'group': self.store_group,
            'store': self.store_name,
            'ctime': {'$lt': ctime_threshold}},
                no_cursor_timeout=True, allow_disk_use=True).batch_size(512) as cursor:
            filecount = mongo_db.files.count_documents({
                'state': 'new',
                'path': self.path_pattern,
                'group': self.store_group,
                'store': self.store_name,
                'ctime': {'$lt': ctime_threshold}})
            logger.info(f"Found {filecount} new files")
            cursor.sort('ctime', ASCENDING)
            ctime_oldfile_threshold = now - int(self.max_age) * 60

            container_list = []
            container = None
            for f in cursor:
                if container is None:
                    try:
                        container = Container(self.archive_size, ctime_oldfile_threshold, self.verify,
                                              self.archive_path)
                    except zipfile.BadZipFile:
                        logger.error(f"Failed to create Container. Going to abort this packing run after finishing the "
                                     f"full container.")
                        container = None
                        break

                try:
                    container.add_file(f)
                except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError) as e:
                    logger.error(f"File could not be added to container {container.name}: {e}")
                if container.is_full():
                    container_list.append(container)
                    container = None

            if container is not None:
                container_list.append(container)

            for container in container_list:
                container.pack()


class Container:
    def __init__(self, archive_size, ctime_threshold, verify, destination_path):
        self.name = str(uuid.uuid1())
        self.filepath = os.path.join(working_directory, "container", self.name)
        self.temp_directory = os.path.join(working_directory, f"tmp-{self.name}")
        self.old_files_mode = False
        self.ctime_threshold = ctime_threshold
        self.archive_size = archive_size
        self.current_size = 0
        self.content = list()
        self.verify = verify
        self.destination_path = destination_path

        try:
            self.archive_file = ZipFile(self.filepath, "w", allowZip64=True)
        except zipfile.BadZipFile as e:
            logger.error(f"Could not create Zipfile for container {self.name}: {e}")
            if os.path.isfile(self.filepath):
                os.remove(self.filepath)
            raise

        logger.info(f"Created new Container {self.name}")

    def add_file(self, file):
        logger.debug(f"Added file {file['pnfsid']} to Container")

        if file['ctime'] < self.ctime_threshold:
            logger.debug("Old files mode activate")
            self.old_files_mode = True

        self.current_size += file['size']
        self.content.append(file)

        try:
            mongo_db.files.update_one({"pnfsid": file['pnfsid']}, {"$set": {"state": f"added: {self.filepath}"}})
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError) as e:
            logger.warning(f"Update of file {file['pnfsid']} in MongoDB failed: {e}")
            raise

    def download_files(self):
        global driver_url
        logger.info(f"Going to download files for archive {self.name} to {self.temp_directory} now.")
        os.mkdir(self.temp_directory)

        for file in self.content:
            count_try = 0
            url = f"{driver_url}/flush"
            headers = {"file": file["replica_uri"]}

            while count_try < 3:
                try:
                    response = requests.get(url, headers=headers, verify=verify)
                    break
                except requests.exceptions.RequestException as e:
                    if count_try < 2:
                        logger.warning(
                            f"Error while downloading file {file['pnfsid']}, try nr. {count_try + 1}: {e}")
                    else:
                        logger.error(f"Downloading file {file['pnfsid']} failed 3 times. Going to abort packing this "
                                     f"container now.")
                        self.reset_mongodb_records()
                        self.current_size = 0
                        self.close()
                        raise

                count_try += 1

            if response.status_code == 200:
                with open(f"{self.temp_directory}/{file['pnfsid']}", "wb") as temp_file:
                    temp_file.write(response.content)
                    logger.debug(f"File {file['pnfsid']} successfully downloaded and written to temp-directory.")
            else:
                logger.error(f"Downloading file {file['pnfsid']} failed with status code {response.status_code}. "
                             f"Abort packing this container.")
                self.reset_mongodb_records()
                raise requests.RequestException(f"Status code not 200, but {response.status_code}")

        logger.info(f"Finished downloading files to {self.temp_directory}")

    def verify_archive(self):
        if self.verify == "filelist":
            verified = len(self.archive_file.filelist) == len(self.content)
        elif self.verify == "chksum":
            self.logger.warning("Checksum verification not implemented yet")
            verified = True
        elif self.verify == 'off':
            verified = True
        else:
            self.logger.warning(f"Unknown verification method {self.verify}. Assuming failure!")
            verified = False

        return verified

    def pack(self):
        logger.info(f"Going to pack {len(self.content)} files with a size of {self.current_size} bytes to container "
                    f"{self.name} now.")
        if self.current_size < self.archive_size and not self.old_files_mode:
            logger.info(f"Archive is not big enough and not in old files mode. Closing Container {self.name}")
            mongo_db.files.update_many({"state": f"added: {self.filepath}"}, {"$set": {"state": "new"}})
            self.close()
            return

        try:
            self.download_files()
        except requests.RequestException as e:
            logger.error(f"Abort packing container {self.name} due to failure of downloading files: {e}")
            self.close()
            return

        for file in os.listdir(self.temp_directory):
            logger.debug(f"Writing file {file} to archive {self.name}")
            self.archive_file.write(os.path.join(self.temp_directory, file), arcname=file)
            mongo_db.files.update_one({"pnfsid": file}, {"$set": {"state": f"added: {self.filepath}"}})
        self.archive_file.close()

        try:
            if self.verify_archive():
                mongo_db.files.update_many({"state": f"added: {self.filepath}"},
                                           {"$set": {"state": f"archived: {self.filepath}"}})
                mongo_db.archives.insert_one({"path": self.filepath, "dest_path": self.destination_path})
                logger.debug(f"Updated file to added for archive {self.name}")
            else:
                mongo_db.files.update_many({"state": f"added: {self.filepath}"}, {"$set": {"state": "new"}})
                logger.debug(f"Updated files to new for {self.name}")
            logger.info(f"Finished packing container {self.name}")
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError) as e:
            logger.critical(f"Could not insert information for container {self.name} to MongoDB: {e}. This might need "
                            f"manual fixing!")

        self.close()

    def close(self):
        logger.info("Closing Container now ...")
        if self.archive_file:
            self.archive_file.close()
            logger.debug(f"Closed archive file {self.name}")

        if os.path.isdir(self.temp_directory):
            logger.debug(f"Removing temp directory")
            for file in os.listdir(self.temp_directory):
                os.remove(os.path.join(self.temp_directory, file))
            os.rmdir(self.temp_directory)

        if not self.is_full() and not self.old_files_mode:
            os.remove(self.filepath)
            logger.debug("Removed unful old container")

        logger.info("Container closed")

    def is_full(self):
        return self.current_size >= self.archive_size

    def reset_mongodb_records(self):
        mongo_db.files.update_many({"state": f"added: {self.filepath}"}, {"$set": {"state": "new"}})


def main(configfile="/etc/dcache/container.conf"):
    global running
    global mongo_url
    global mongo_db
    global working_directory
    global script_id
    global loop_delay
    global logger
    global driver_url

    log_handler = None

    while running:
        configuration = get_config(configfile)

        log_level_str = configuration.get('DEFAULT', 'log_level')
        mongo_db_name = configuration.get('DEFAULT', 'mongo_db')

        log_level = getattr(logging, log_level_str.upper(), None)
        logger.setLevel(log_level)

        if log_handler is not None:
            log_handler.close()
            logger.removeHandler(log_handler)

        log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/pack-files-{script_id}.log')
        formatter = logging.Formatter('%(asctime)s %(name)-10s %(levelname)-8s %(message)s')
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)

        logger.info(f"Successfully read configuration from file {configfile}.")

        logger.debug(f"script_id: {script_id}")
        logger.debug(f"mongo_url: {mongo_url}")
        logger.debug(f"mongo_db: {mongo_db_name}")
        logger.debug(f"working_dir: {working_directory}")
        logger.debug(f"log_level: {log_level}")
        logger.debug(f"loop_delay: {loop_delay}")
        logger.debug(f"driver_url: {driver_url}")

        try:
            mongo_client = MongoClient(mongo_url)
            mongo_db = mongo_client[mongo_db_name]
            logger.info("Established connection to MongoDB")

            logger.info("Sanitizing database")
            mongo_db.files.update_many({'lock': script_id}, {'$set': {'state': 'new'}, '$unset': {'lock': ""}})
        except (pymongo.errors.ConnectionFailure, pymongo.errors.InvalidURI, pymongo.errors.InvalidName,
                pymongo.errors.ServerSelectionTimeoutError) as e:
            logger.warning(f"Connection to MongoDB failed: {e}")
            logger.info(f"Sleeping now for {loop_delay} seconds.")
            time.sleep(loop_delay)
            continue

        for group in configuration.sections():
            logger.debug(f"Group: {group}")
            Group(group, configuration)

        logger.info(f"Script finished packing run, going to sleep {loop_delay} seconds now.")
        time.sleep(loop_delay)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    sys.excepthook = uncaught_handler
    if not os.getuid() == 0:
        print("pack-files must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: pack-files.py <configfile>")
        sys.exit(1)

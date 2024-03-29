#!/usr/bin/env python3
# coding=utf-8

import configparser as parser
import datetime
import logging
import logging.handlers
import json
import os
import pymongo.errors
from pymongo import MongoClient
import requests
import signal
import sys
import time
import traceback
import zipfile

running = True
logger = logging.getLogger()
working_dir = ""
verify = True


def sigint_handler(signum, frame):
    global running
    print(f"Caught signal {signum}.")
    logging.info(f"Caught signal {signum}.")
    running = False


def uncaught_handler(*exc_info):
    err_text = "".join(traceback.format_exception(*exc_info))
    logging.critical(err_text)
    sys.stderr.write(err_text)


def read_config(configfile):
    global working_dir
    global verify
    # function reads config and returns object
    configuration = parser.RawConfigParser(defaults={'scriptId': 'pack', 'mongoUri': 'mongodb://localhost/',
                                                     'mongoDb': 'smallfiles', 'logLevel': 'ERROR'})
    try:
        if not os.path.isfile(configfile):
            raise FileNotFoundError
        configuration.read(configfile)
        script_id = configuration.get('DEFAULT', 'script_id')
        log_level_str = configuration.get('DEFAULT', 'log_level')
        mongo_db = configuration.get('DEFAULT', 'mongo_db')
        working_dir = configuration.get("DEFAULT", "working_dir")
        keep_archive_time = configuration.get("DEFAULT", "keep_archive_time")
        verify_str = configuration.get('DEFAULT', 'verify')
        if verify_str == "":
            verify = verify
        elif verify_str in ("False", "false"):
            verify = False
        elif verify_str in ("True", "true"):
            verify = True
        else:
            verify = verify_str
    except FileNotFoundError:
        print(f'Configuration file "{configfile}" not found.')
        raise
    except parser.NoSectionError:
        print(
            f'Section [DEFAULT] was not found in "{configfile}". This section is mandatory.')
        raise
    except parser.NoOptionError as e:
        print(f'An option is missing in section [DEFAULT] of file "{configfile}", exiting now: {e}')
        raise
    except KeyError as e:
        print(f"There's something wrong with a key, {e}")
        raise
    except parser.MissingSectionHeaderError:
        print(f'The file "{configfile}" doesn\'t contain section headers. Exiting now')
        raise
    except parser.ParsingError as e:
        print(
            f'There was an error parsing while parsing the configuration "{configfile}", exiting now: {e}')
        raise
    except parser.DuplicateSectionError as e:
        print(f"There are duplicated sections: {e}")
        raise
    except parser.DuplicateOptionError as e:
        print(f"There are duplicated options: {e}")
        raise
    except parser.Error as e:
        print(f'An error occurred while reading the configuration file {configfile}, exiting now: {e}')
        raise

    # Check if values are empty
    if script_id == "":
        print(f"script_id is empty")
        raise ValueError("script_id is empty")
    if configuration.get("DEFAULT", "mongo_url") == "":
        print(f"mongo_url is empty")
        raise ValueError("mongo_url is empty")
    if mongo_db == "":
        print(f"mongo_db is empty")
        raise ValueError("mongo_db is empty")
    if working_dir == "":
        print(f"working_dir is empty")
        raise ValueError("working_dir is empty")
    if not os.path.exists(working_dir):
        os.mkdir(working_dir)
    working_dir = f"{working_dir}/stage-tmp"
    if not os.path.exists(working_dir):
        os.mkdir(working_dir)
    if configuration.get("DEFAULT", "webdav_door") == "":
        print(f"webdav_door is empty")
        raise ValueError("webdav_door is empty")
    if configuration.get("DEFAULT", "frontend") == "":
        print(f"frontend is empty")
        raise ValueError("frontend is empty")
    if configuration.get("DEFAULT", "macaroon") == "":
        print(f"macaroon is empty")
        raise ValueError("macaroon is empty")
    if keep_archive_time == "":
        print(f"keep_archive_time is empty")
        raise ValueError("keep_archive_time is empty")

    # Check if values are valid
    if any(i in script_id for i in ["/", "$", "\\00"]):
        print("script_id contains chars that are not valid")
        raise ValueError("script_id contains invalid chars like /, $ or \\00")

    if log_level_str.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        print("Log level is invalid")
        raise ValueError(f"Invalid log_level {log_level_str}. Must be one of (DEBUG|INFO|WARNING|ERROR|CRITICAL)")

    if '.' in mongo_db:
        print("Invalid database name")
        raise ValueError("mongo_db contains an invalid charakter like '.'")

    if not int(keep_archive_time):
        raise ValueError("keep_archive_time could not be converted to an integer. Please check if this value is a number!")
    return configuration


def get_archive_path(pnfsid, headers, frontend):
    global verify
    logger.debug(f"Called get_archive_path for {pnfsid}")
    url = f"{frontend}/api/v1/id/{pnfsid}"
    response = requests.get(url, headers=headers, verify=verify)
    if response.status_code == 200:
        json_response = json.loads(response.content.decode("utf-8"))
        return json_response['path'], 200
    else:
        logger.error(f"Could not get archive path, status code: {response.status_code}")
        return None, response.status_code


def download_archive(archive, webdav_door, frontend, macaroon, tmp_path):
    global verify
    logger.debug(f"Called download_archive for {archive}")
    headers = {"Content-Type": "application/octet-stream",
               "Authorization": f"Bearer {macaroon}"}
    archive_path, archive_response = get_archive_path(archive, headers, frontend)
    if archive_response == 401:
        return 401
    elif archive_path is None:
        return 1
    url = f"{webdav_door}/{archive_path}"
    response = requests.get(url, headers=headers, verify=verify)
    global working_dir

    if response.status_code == 200:
        open(f"{working_dir}/{archive}", "wb").write(response.content)
        logger.info(f"Download archive {archive} successfully")
    elif response.status_code == 401:
        logger.error(f"The given macaroon is invalid or expired.")
    else:
        logging.error(f"Error: Downloading archive failed with code {response.status_code}")
    return response.status_code


def extract_archive(location):
    return location.split(":")[-1]


def unpack_upload_file(archive, pnfsid, filepath, url, mongo_db, macaroon):
    global verify
    with zipfile.ZipFile(os.path.join(working_dir, archive)) as zip_file:
        files = {"file": (pnfsid, zip_file.read(pnfsid), "text/plain")}
        data, content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
        headers = {"Content-Type": content_type, "file": filepath, "Authorization": f"Bearer {macaroon}"}

        response = requests.post(url, data=data, headers=headers, verify=verify)
        logger.debug(f"Upload status code: {response.status_code}")

        stat = os.stat(os.path.join(working_dir, archive))
        os.utime(os.path.join(working_dir, archive),
                 times=(datetime.datetime.now().timestamp(), stat.st_mtime))  # Update access time of archive

        if response.status_code == 201:
            try:
                mongo_db.stage.update_one({"pnfsid": pnfsid}, {"$set": {"status": "done"}})
            except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError) as e:
                logger.error(f"Could not set record for {pnfsid} to done, caused by: {e}")
                return False
            return True
        else:
            logger.warning(f"File {pnfsid} could not be uploaded to dCache. Code: {response.status_code}")
            return False


def cleanup_archives(keep_archive_time):
    global working_dir
    # Keep archives for <keep_archive_time> minutes before removing them
    existing_archives = os.listdir(working_dir)
    logger.debug(f"Existing archives: {existing_archives}")
    for archive in existing_archives:
        last_access_time = os.path.getatime(os.path.join(working_dir, archive))
        time_threshold = datetime.datetime.timestamp(datetime.datetime.now() -
                                                     datetime.timedelta(minutes=keep_archive_time))

        if last_access_time < time_threshold:
            logger.info(f"Archive {archive} was not accessed since {keep_archive_time} minutes and will be deleted now.")
            os.remove(os.path.join(working_dir, archive))


def main(config="/etc/dcache/container.conf"):
    global running
    global logger
    global working_dir
    log_handler = None

    while running:
        configuration = read_config(config)

        mongo_uri = configuration.get('DEFAULT', 'mongo_url')
        mongo_db_name = configuration.get('DEFAULT', 'mongo_db')
        webdav_door = configuration.get("DEFAULT", "webdav_door")
        frontend = configuration.get("DEFAULT", "frontend")
        macaroon_path = configuration.get("DEFAULT", "macaroon")
        with open(macaroon_path, "r") as macaroon_file:
            macaroon = macaroon_file.read().strip()
        log_level_str = configuration.get("DEFAULT", "log_level")
        script_id = configuration.get("DEFAULT", "script_id")
        keep_archive_time = configuration.get("DEFAULT", "keep_archive_time")
        keep_archive_time = int(keep_archive_time)

        log_level = getattr(logging, log_level_str.upper(), None)
        logger.setLevel(log_level)

        if log_handler is not None:
            log_handler.close()
            logger.removeHandler(log_handler)

        log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/stage-{script_id}.log')
        formatter = logging.Formatter('%(asctime)s %(name)-10s %(levelname)-8s %(message)s')
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)

        logger.debug(f"Script ID: {script_id}")
        logger.debug(f"Log level: {log_level_str}")
        logger.debug(f"Mongo URI: {mongo_uri}")
        logger.debug(f"Mongo database: {mongo_db_name}")
        logger.debug(f"Webdav Door: {webdav_door}")
        logger.debug(f"Macaroon: {macaroon}")
        logger.debug(f"Keep archive time: {keep_archive_time}")

        logger.info(f'Successfully read configuration from file {config}.')

        try:
            client = MongoClient(mongo_uri)
            session = client.start_session()
            mongo_db = client[mongo_db_name]

            results = mongo_db.stage.find({"status": "new"}, no_cursor_timeout=True,
                                          allow_disk_use=True, batch_size=1024, session=session)
            length_results = mongo_db.stage.count_documents({"status": "new"})
        except (pymongo.errors.ConnectionFailure, pymongo.errors.InvalidURI, pymongo.errors.InvalidName,
                pymongo.errors.ServerSelectionTimeoutError) as e:
            logger.error(f"Connection to MongoDB failed, sleeping 30s now: {e}")
            time.sleep(30)
            continue

        if length_results == 0:
            logger.info(f"Found no files to be staged in MongoDB. Sleeping 30s now")
            cleanup_archives(keep_archive_time)
            time.sleep(30)
            continue
        else:
            logger.info(f"Found {length_results} files to be staged.")

        if not os.path.exists(working_dir):
            os.mkdir(working_dir)

        download_code = -1
        for request in results:
            if not running:
                logger.info(f"Stopping script due to running set to false")
                return

            locations = request['locations']
            pnfsid = request['pnfsid']
            url = f"{request['driver_url']}/v1/stage"
            logger.debug(f"File {pnfsid}, url {url}")

            logger.debug(f"File {pnfsid} has {len(locations)} locations.")

            for location in locations:
                archive = extract_archive(location)
                logger.debug(f"location: {location}\nextracted archive: {archive}")
                if not os.path.isfile(os.path.join(working_dir, archive)):
                    download_code = download_archive(archive, webdav_door, frontend, macaroon, working_dir)
                    if download_code not in (200, 401):
                        continue
                    elif download_code == 401:
                        break
                else:
                    download_code = 1
                if unpack_upload_file(archive, pnfsid, request["filepath"], url, mongo_db, macaroon):
                    logger.info(f"File {pnfsid} was uploaded to dCache successfully")
                    break
                else:
                    logger.error(f"Unpacking and uploading file {pnfsid} did not work!")

            logger.debug(f"Download code is {download_code}")

            if download_code not in (200, 401, 1):
                logger.error(f"No working location found for file {pnfsid}!")
                try:
                    mongo_db.stage.update_one({"pnfsid": pnfsid}, {"$set": {"status": "failure"}})
                except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError) as e:
                    logger.error(f"Could not set record for {pnfsid} to failure, caused by: {e}")
                    break
            elif download_code == 401:
                logger.error("The macaroon being used is invalid or expired, please renew it!")
                break

        logger.debug("finished, tidy up")
        results.close()
        cleanup_archives(keep_archive_time)
        logger.info("Finished run, sleeping now for 30 s")
        time.sleep(30)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    sys.excepthook = uncaught_handler
    if not os.getuid() == 0:
        print("stage-files must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: pack-files.py <configfile>")
        sys.exit(1)

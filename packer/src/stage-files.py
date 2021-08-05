import bson
from bson.son import SON
import configparser as parser
import datetime
import logging
import logging.handlers
import json
import os
import pymongo.errors
from pymongo import MongoClient
import requests
import shutil
import signal
import sys
import time
import traceback
import zipfile

running = True
logger = logging.getLogger()
working_dir = ""


def sigint_handler(signum, frame):
    global running
    print(f"Caught signal {signum}.")
    logging.info(f"Caught signal {signum}.")
    running = False


def uncaught_handler(*exc_info):
    err_text = "".join(traceback.format_exception(*exc_info))
    print(err_text)
    sys.stderr.write(err_text)


def read_config(configfile):
    global working_dir
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
        working_dir = f"{working_dir}/stage-tmp"
        stage_wait_min = configuration.get("DEFAULT", "stage_wait_min")
        stage_wait_max = configuration.get("DEFAULT", "stage_wait_max")
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

    try:
        float(stage_wait_min)
    except ValueError:
        logger.error(f"stage_wait_min in configuration is not a number: {stage_wait_min}")
    try:
        float(stage_wait_max)
    except ValueError:
        logger.error(f"stage_wait_max in configuration is not a number: {stage_wait_max}")

    if stage_wait_min > stage_wait_max:
        logger.error(f"stage_wait_min is bigger than stage_wait_max!")
        raise ValueError("stage_wait_min is bigger than stage_wait_max")

    return configuration


def get_archive_path(pnfsid, headers):
    logger.debug(f"Called get_archive_path for {pnfsid}")
    url = f"https://os-smeyer-dcache-test02.desy.de:3880/api/v1/id/{pnfsid}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        json_response = json.loads(response.content.decode("utf-8"))
        return json_response['path']
    else:
        logger.error(f"Could not get archive path, status code: {response.status_code}")
        return None


def download_archive(archive, webdav_door, macaroon, tmp_path):
    logger.debug(f"Called download_archive for {archive}")
    headers = {"Content-Type": "application/octet-stream",
               "Authorization": f"Bearer {macaroon}"}
    archive_path = get_archive_path(archive, headers)
    url = f"{webdav_door}/{archive_path}"
    response = requests.get(url, headers=headers)
    global working_dir

    if response.status_code == 200:
        open(f"{working_dir}/{archive}", "wb").write(response.content)
        logger.info(f"Downloade archive {archive} successfully")
        return True
    else:
        logging.error(f"Error: Downloading archive failed with code {response.status_code}")
        return False


def main(config="/etc/dcache/container.conf"):
    global running
    global logger
    global working_dir
    log_handler = None

    while running:
        configuration = read_config(config)

        mongo_uri = configuration.get('DEFAULT', 'mongo_url')
        mongo_db = configuration.get('DEFAULT', 'mongo_db')
        webdav_door = configuration.get("DEFAULT", "webdav_door")
        macaroon = configuration.get("DEFAULT", "macaroon")
        driver_url = configuration.get("DEFAULT", "driver_url")
        log_level_str = configuration.get("DEFAULT", "log_level")
        script_id = configuration.get("DEFAULT", "script_id")
        stage_wait_min = configuration.get("DEFAULT", "stage_wait_min")
        stage_wait_max = configuration.get("DEFAULT", "stage_wait_max")

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
        logger.debug(f"Mongo database: {mongo_db}")
        logger.debug(f"Webdav Door: {webdav_door}")
        logger.debug(f"Macaroon: {macaroon}")
        logger.debug(f"Driver URL: {driver_url}")

        logger.info(f'Successfully read configuration from file {config}.')

        try:
            client = MongoClient(mongo_uri)
            db = client[mongo_db]

            # Get records for stage, sorted by archive
            pipeline = [{"$unwind": "$archive"},
                        {"$match": {"status": "new"}},
                        {"$sort": SON([("archive", -1)])}]
            results = list(db.stage.aggregate(pipeline))
        except (pymongo.errors.ConnectionFailure, pymongo.errors.InvalidURI, pymongo.errors.InvalidName,
                pymongo.errors.ServerSelectionTimeoutError) as e:
            logger.error(f"Connection to MongoDB failed, sleeping 30s now: {e}")
            time.sleep(30)
            continue

        if len(results) == 0:
            logger.info(f"Found no files to be staged in MongoDB. Sleeping 30s now")
            time.sleep(30)
            continue
        else:
            logger.info(f"Found {len(results)} files to be staged.")

        archive_files = dict()

        # Group results by archive
        for element in results:
            if element['archive'] not in archive_files.keys():
                archive_files[element['archive']] = {}
            archive_files[element['archive']][element['pnfsid']] = element['filepath']

        if not os.path.exists(f"{working_dir}"):
            os.mkdir(f"{working_dir}")

        for archive in archive_files:
            if not running:
                logger.debug("Running is set to false")
                shutil.rmtree(working_dir)
                return
            logger.info(f"Found {len(archive_files[archive])} files for archive {archive}")

            min_threshold = datetime.datetime.now() - datetime.timedelta(seconds=float(stage_wait_min))
            max_threshold = datetime.datetime.now() - datetime.timedelta(seconds=float(stage_wait_max))
            min_threshold = bson.ObjectId.from_datetime(min_threshold)
            max_threshold = bson.ObjectId.from_datetime(max_threshold)

            files_too_new = True if len(list(db.stage.find({"$and": [{"_id": {"$gt": min_threshold}}, {"archive": archive}]}))) > 0 else False
            files_too_old = True if len(list(db.stage.find({"$and": [{"_id": {"$lt": max_threshold}}, {"archive": archive}]}))) > 0 else False

            if files_too_new and not files_too_old:
                logger.info(f"There are records for archive {archive} that are too young, but no records that are too "
                            f"old. Skipping archive for this run.")
                continue

            # Download archive and unpack files in archive
            tmp_path = f"{working_dir}/{archive}-tmp"
            os.mkdir(tmp_path)

            if not download_archive(archive, webdav_door, macaroon, tmp_path):
                logger.error("Downloading archive failed.")
                continue

            url = f"{driver_url}/stage"
            with zipfile.ZipFile(f"{working_dir}/{archive}", "r") as zip_file:
                for file in archive_files[archive].keys():
                    files = {'file': (file, zip_file.read(file), "text/plain")}
                    # TODO Replace so that no private function is used:
                    data, content_type = requests.models.RequestEncodingMixin._encode_files(files, {})
                    headers = {"Content-Type": content_type, "file": archive_files[archive][file]}

                    response = requests.post(url, data=data, headers=headers)
                    logger.debug(f"Upload status code: {response.status_code}")
                    if response.status_code == 200:
                        logger.info(f"File {file} was uploaded to dCache successfully")
                        db.stage.update_one({"pnfsid": file}, {"$set": {"status": "done"}})
                    else:
                        logger.warning(f"File {file} could not be uploaded to dCache.")
            shutil.rmtree(f"{working_dir}/{archive}-tmp")

        logger.debug("finished, tidy up")
        shutil.rmtree(working_dir)
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

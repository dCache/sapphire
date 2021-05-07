#!/usr/bin/env python3
# coding=utf-8
import os
import sys
import time
import signal
from zlib import adler32
from hashlib import md5, sha1
from zipfile import ZipFile, BadZipfile

import pymongo.errors
from pymongo import MongoClient, errors
import configparser as parser
import logging
import logging.handlers
import traceback
import requests
import base64

running = True


def sigint_handler(signum, frame):
    global running
    logging.info(f"Caught signal {signum}.")
    print(f"Caught signal {signum}.")
    running = False


def uncaught_handler(*exc_info):
    err_text = "".join(traceback.format_exception(*exc_info))
    logging.critical(err_text)
    sys.stderr.write(err_text)


def _md5(filepath):
    md5_value = md5()
    with open(filepath, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            md5_value.update(chunk)
    return base64.b64encode(md5_value.digest()).decode()


def _adler32(filepath):
    blocksize = 256*1024*1024
    adler32_value = 1
    with open(filepath, "rb") as file:
        while True:
            data = file.read(blocksize)
            if not data:
                break
            adler32_value = adler32(data, adler32_value)
            if adler32_value < 0:
                adler32_value += 2**32
    checksum = hex(adler32_value)[2:]
    while len(checksum) < 8:
        checksum = f"0{checksum}"
    return checksum


def _sha1(filepath):
    sha1_value = sha1()
    with open(filepath, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            sha1_value.update(chunk)
    return sha1_value.hexdigest()


def get_config(configfile):
    # function reads config and returns object
    configuration = parser.RawConfigParser(defaults={'scriptId': 'pack', 'mongoUri': 'mongodb://localhost/',
                                                     'mongoDb': 'smallfiles', 'logLevel': 'ERROR'})

    try:
        if not os.path.isfile(configfile):
            raise FileNotFoundError
        configuration.read(configfile)
        script_id = configuration.get('DEFAULT', 'script_id')
        log_level_str = configuration.get('DEFAULT', 'log_level')
        mongo_uri = configuration.get('DEFAULT', 'mongo_url')
        mongo_db = configuration.get('DEFAULT', 'mongo_db')
        webdav_door = configuration.get('DEFAULT', 'webdav_door')
        macaroon = configuration.get('DEFAULT', 'macaroon')
    except FileNotFoundError as e:
        logging.critical(f'Configuration file "{configfile}" not found.')
        raise
    except parser.NoSectionError as e:
        logging.critical(
            f'Section [DEFAULT] was not found in "{configfile}". This section is mandatory.')
        raise
    except parser.NoOptionError as e:
        logging.critical(f'An option is missing in section [DEFAULT] of file "{configfile}", exiting now: {e}')
        raise
    except KeyError as e:
        logging.critical(f"There's something wrong with a key, {e}")
        raise
    except parser.MissingSectionHeaderError as e:
        logging.critical(f'The file "{configfile}" doesn\'t contain section headers. Exiting now')
        raise
    except parser.ParsingError as e:
        logging.critical(
            f'There was an error parsing while parsing the configuration "{configfile}", exiting now: {e}')
        raise
    except parser.DuplicateSectionError as e:
        logging.critical(f"There are duplicated sections: {e}")
        raise
    except parser.DuplicateOptionError as e:
        logging.critical(f"There are duplicated options: {e}")
        raise
    except parser.Error as e:
        logging.critical(f'An error occurred while reading the configuration file {configfile}, exiting now: {e}')
        raise

    # Check if values are valid
    if any(i in script_id for i in ["/", "$", "\\00"]):
        logging.error("script_id contains chars that are not valid")
        raise ValueError("script_id contains invalid chars like /, $ or \\00")
    if log_level_str.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        logging.error("Log level is invalid")
        raise ValueError(f"Invalid log_level {log_level_str}. Must be one of (DEBUG|INFO|WARNING|ERROR|CRITICAL)")
    if '.' in mongo_db:
        logging.error("Invalid database name")
        raise ValueError("mongo_db contains an invalid charakter like '.'")
    return configuration


def reset_pnfsid(pnfsid, db):
    db_file = db.files.find_one({"pnfsid": pnfsid})
    db_file['state'] = "new"
    db.files.replace_one({"pnfsid": pnfsid}, db_file)


def main(configfile='/etc/dcache/container.conf'):
    # global variables
    global running

    # general variables
    checksum_calculation = {"md5": _md5,
                            "adler32": _adler32,
                            "sha1": _sha1
                            }
    logger = logging.getLogger()
    log_handler = None

    while running:
        # Read configuration
        configuration = get_config(configfile)

        script_id = configuration.get('DEFAULT', 'script_id')
        log_level_str = configuration.get('DEFAULT', 'log_level')
        mongo_uri = configuration.get('DEFAULT', 'mongo_url')
        mongo_db = configuration.get('DEFAULT', 'mongo_db')
        webdav_door = configuration.get('DEFAULT', 'webdav_door')
        macaroon = configuration.get('DEFAULT', 'macaroon')

        log_level = getattr(logging, log_level_str.upper(), None)
        logger.setLevel(log_level)

        if log_handler is not None:
            log_handler.close()
            logger.removeHandler(log_handler)

        log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/verify_container-{script_id}.log')
        formatter = logging.Formatter('%(asctime)s %(name)-10s %(levelname)-8s %(message)s')
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)

        logger.debug(f"Script ID: {script_id}")
        logger.debug(f"Log level: {log_level_str}")
        logger.debug(f"Mongo URI: {mongo_uri}")
        logger.debug(f"Mongo database: {mongo_db}")
        logger.debug(f"Webdav Door: {webdav_door}")
        logger.debug(f"Macaroon: {macaroon}")

        logger.info(f'Successfully read configuration from file {configfile}.')

        # Connect to database and get archives
        try:
            client = MongoClient(mongo_uri)
            db = client[mongo_db]
            with db.archives.find() as db_archives:
                skip = False
                logger.info(f"Found {db.archives.count_documents({})} new archives")
                for archive in db_archives:
                    if not running:
                        logger.info("Exiting")
                        sys.exit(0)
                    url = f"{webdav_door}/{archive['dest_path']}/{os.path.basename(archive['path'])}"
                    # Open ZIP-File and get filelist
                    logger.info(f"Processing archive {archive['path']}")
                    try:
                        zip_file = ZipFile(archive['path'], mode="r", allowZip64=True)
                        archive_pnfsidlist = [f.filename for f in zip_file.filelist]
                    except BadZipfile:
                        logger.warning(f"Archive {archive['path']} is not ready yet. Will try again later.")
                        continue
                    except FileNotFoundError:
                        logger.error(
                            f"Container {archive['path']} could not be found on local disk. Files, that should be "
                            f"in this archive, are now reset to be packed again.")
                        for archived in db.files.find({"state": f"archived: {archive['path']}"}):
                            pnfsid = archived['pnfsid']
                            reset_pnfsid(pnfsid, db)
                            # file_result = db.files.find_one({"pnfsid": pnfsid})
                            # file_result['state'] = "new"
                            # db.files.replace_one({"pnfsid": pnfsid}, file_result)
                            logger.debug(f"Resetted file with PNFSID {pnfsid}")
                        db.archives.delete_one({"path": archive['path']})
                        continue
                    finally:
                        if "zip_file" in locals():
                            zip_file.close()
                            logger.debug("File closed")

                    # Check if every file that should be in the archive is there
                    db_pnfsidlist = db.files.find({"state": f"archived: {archive['path']}"})
                    db_pnfsidlist = [f['pnfsid'] for f in db_pnfsidlist]
                    sym_diff_pnfsidlist = set(archive_pnfsidlist).symmetric_difference(set(db_pnfsidlist))
                    logger.info(f"There were {len(sym_diff_pnfsidlist)} files with problems in archive")
                    for pnfsid in sym_diff_pnfsidlist:
                        if pnfsid in archive_pnfsidlist:
                            logger.warning(f"File {pnfsid} is in archive {archive['path']}, but not in MongoDB! "
                                           f"Creating new entry to failures collection")
                            db.failures.insert_one({'archivePath': archive['path'], 'pnfsid': pnfsid})
                        elif pnfsid in db_pnfsidlist:
                            logger.warning(f"File {pnfsid} is listed in MongoDB to be in archive {archive['path']}, "
                                           f"but isn't there. Resetting file for packing into new archive")
                            reset_pnfsid(pnfsid, db)
                            # db_file = db.files.find_one({"pnfsid": pnfsid})
                            # db_file['state'] = "new"
                            # db.files.replace_one({"pnfsid": pnfsid}, db_file)

                    # Upload zip-file to dCache
                    headers = {"Content-type": "application/octet-stream",
                               "Authorization": f"Bearer {macaroon}"}
                    retry_counter = 0
                    response_status_code = 0
                    while retry_counter <= 3 and response_status_code not in (200, 201):
                        # Check if file is already uploaded
                        if retry_counter == 0:
                            headers = {"Authorization": f"Bearer {macaroon}", "Want-Digest": "ADLER32,MD5,SHA1"}
                            response = requests.head(url, verify=True, headers=headers)
                            if response.status_code in (200, 204):
                                checksum_type, remote_checksum = response.headers.get("Digest").split('=', 1)
                                local_checksum = checksum_calculation[checksum_type.lower()](archive['path'])
                                if local_checksum == remote_checksum:
                                    logger.info(f"File already exists on dCache, code {response.status_code}")
                                else:
                                    logger.error(f"File already exists on dCache, but with a different checksum!! "
                                                 f"Local checksum: {local_checksum} ; Remote checksum: "
                                                 f"{remote_checksum} ; Type: {checksum_type}")
                                    # Create MongoDB Entry for Archive
                                    db.archive_failure.insert_one({"pnfsid": response.headers.get("ETag").split('_')[0]
                                                                  .replace('"', ''),
                                                                   "location": archive['path'],
                                                                   "files": archive_pnfsidlist})
                                    # Reset files
                                    for pnfsid in archive_pnfsidlist:
                                        db_file = db.files.find_one({"pnfsid": pnfsid})
                                        if db_file is None:
                                            logger.info(f"File {pnfsid} from archive {archive['path']} is no longer in "
                                                        f"MongoDB. Maybe it was removed from Java-Driver")
                                        else:
                                            reset_pnfsid(pnfsid, db)
                                            # db_file['state'] = "new"
                                            # db.files.replace_one({"pnfsid": pnfsid}, db_file)
                                    # Delete local file
                                    db.archives.delete_one({"path": archive['path']})
                                    try:
                                        os.remove(archive['path'])
                                    except FileNotFoundError as e:
                                        logger.warning(
                                            f"Archive {archive['path']} could not be removed as the file was not found.")
                                    logger.debug(f"Deleted local archive {archive['path']}")
                                    skip = True
                            else:
                                try:
                                    response = requests.put(url, data=open(archive['path'], 'rb'), verify=True, headers=headers)
                                except (ConnectionError, TimeoutError, requests.exceptions.RequestException) as e:
                                    logger.error(f"An exception occured while uploading zip-file to dCache. Will retry in a "
                                                 f"few seconds: {e}")
                                    retry_counter += 1
                                    time.sleep(10)
                                    continue
                        response_status_code = response.status_code
                        if not skip:
                            logger.debug(f"Uploading zip-file finished with status code {response_status_code}")
                        if response_status_code not in (200, 201):
                            logger.info(f"Uploading file to dCache failed as the returned status code, "
                                        f"{response_status_code}, is not 200 or 201. Retrying in a few seconds.")
                            retry_counter += 1
                            time.sleep(10)
                    if skip:
                        logger.debug(f"Skipping archive")
                        continue
                    if retry_counter == 4:
                        logger.critical(f"Zip-file could not be uploaded to dCache, even after retrying "
                                        f"{retry_counter - 1} time(s). Please check your dCache! Exiting script now...")
                        sys.exit(1)

                    # Request PNFSID and Checksum from dCache, calculate local checksum
                    headers = {"Want-Digest": "ADLER32,MD5,SHA1",
                               "Authorization": f"Bearer {macaroon}"}
                    retry_counter = 0
                    response_status_code = 0
                    while retry_counter <= 3 and response_status_code not in (200, 201):
                        try:
                            response = requests.head(url, verify=True, headers=headers)
                        except Exception as e:
                            logger.error(f"An exception occured while requesting checksum and pnfsid. Will retry in a "
                                         f"few seconds: {e}")
                            retry_counter += 1
                            time.sleep(10)
                            continue
                        response_status_code = response.status_code
                        logger.debug(f"Requesting checksum and pnfsid finished with status code {response_status_code}")
                        if response_status_code not in (200, 201):
                            logger.info(f"Requesting checksum and pnfsid failed as the returned status code, "
                                        f"{response_status_code}, is not 200 or 201. Retrying in a few seconds.")
                            retry_counter += 1
                            time.sleep(10)
                    if retry_counter == 4:
                        logger.critical(
                            f"Checksum and pnfsid of zip-file could not be requested from dCache, even after "
                            f"retrying {retry_counter - 1} time(s). Please chack your dCache! Exiting script "
                            f"now...")
                        sys.exit(1)

                    archive_pnfsid = response.headers.get("ETag").split('_')[0].replace('"', '')
                    checksum_type, remote_checksum = response.headers.get("Digest").split('=', 1)
                    if str.lower(checksum_type) not in checksum_calculation.keys():
                        logger.error(f"Checksum type {checksum_type} is not implemented!")
                        raise NotImplementedError()
                    try:
                        local_checksum = checksum_calculation[checksum_type](archive['path'])
                    except FileNotFoundError as e:
                        logger.error(f"Archive {archive['path']} could not be found for checksum calculation! Deleting "
                                     f"uploaded file and resetting files that should be in this archive to get packed "
                                     f"again.")
                        for pnfsid in archive_pnfsidlist:
                            reset_pnfsid(pnfsid, db)
                            # file_result = db.files.find_one({"pnfsid": pnfsid})
                            # file_result['state'] = "new"
                            # db.files.replace_one({"pnfsid": pnfsid}, file_result)
                            logger.debug(f"Resetted file with PNFSID {pnfsid}")
                        headers = {"Authorization": f"Bearer {macaroon}"}
                        response = requests.delete(url, headers=headers, verify=True)
                        if response.status_code == 204:
                            logger.info(f"Archive was successfully deleted from dCache.")
                        else:
                            logger.info(f"Archive wasn't deleted from dCache, status code: {response.status_code}")

                        db.archives.delete_one({"path": archive['path']})
                        continue

                    # Compare Checksums
                    count_updated = 0
                    logger.debug(f"Checksum of archive locally: {local_checksum}, on dCache: {remote_checksum}; "
                                 f"Checksum type: {checksum_type}")
                    if remote_checksum == local_checksum:
                        for file_pnfsid in archive_pnfsidlist:
                            if file_pnfsid in sym_diff_pnfsidlist:
                                logger.debug(f"File {file_pnfsid} is in list with problems, continuing.")
                                continue
                            file_entry = db.files.find_one({'pnfsid': file_pnfsid})
                            if file_entry is None:
                                continue
                            hsm_type = file_entry['hsm_type']
                            hsm_name = file_entry['hsm_name']
                            archive_url = f"{hsm_type}://{hsm_name}/?store={file_entry['store']}&group=" \
                                          f"{file_entry['group']}&bfid={file_pnfsid}:{archive_pnfsid}"
                            file_entry['archiveUrl'] = archive_url
                            file_entry['state'] = f"verified: {archive['path']}"
                            db.files.replace_one({"pnfsid": file_pnfsid}, file_entry)
                            count_updated += 1
                            logger.debug(f"Updated file with pnfsid {file_pnfsid}")
                        logger.info(f"Updated {count_updated} file records in MongoDB")
                    else:
                        logger.error(
                            f"Checksums of local and remote zip-file didn't match. Going to delete remote archive "
                            f"to reupload it next run.")
                        # delete file on dCache
                        headers = {"Authorization": f"Bearer {macaroon}"}
                        response = requests.delete(url, headers=headers, verify=True)

                        if response.status_code == 204:
                            logger.info(f"Archive was successfully deleted from dCache.")
                        else:
                            logger.info(f"Archive wasn't deleted from dCache, status code: {response.status_code}")
                        continue

                    # Cleanup
                    try:
                        os.remove(archive['path'])
                    except FileNotFoundError as e:
                        logger.warning(f"Archive {archive['path']} could not be removed as the file was not found.")
                    logger.debug(f"Deleted local archive {archive['path']}")
                    db.archives.delete_one({"path": archive['path']})
                    logger.debug(f"Removed MongoDB record in archives for archive {archive['path']}")
                    logger.info(f"Finished processing {archive['path']}")
                if client is not None:
                    client.close()
        except (pymongo.errors.ConnectionFailure, pymongo.errors.InvalidURI, pymongo.errors.InvalidName,
                pymongo.errors.ServerSelectionTimeoutError) as e:
            logger.error(f"Connection to {mongo_uri}, database {mongo_db}, failed. Will retry in next iteration "
                         f"again. {e}")
            time.sleep(60)
            continue

        time.sleep(60)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    sys.excepthook = uncaught_handler
    if not os.getuid() == 0:
        print("verify_container.py must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: verify_container.py <configfile>")
        sys.exit(2)

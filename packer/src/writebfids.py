#!/usr/bin/env python3
# coding=utf-8
import os
import sys
import time
import signal
from zlib import adler32
from hashlib import md5, sha1
from zipfile import ZipFile, BadZipfile
from pymongo import MongoClient, errors
import configparser as parser
import logging
import logging.handlers
import traceback
import requests
from requests.auth import HTTPBasicAuth
import base64

running = True

archiveUser = ""
archiveMode = ""
mountPoint = ""
dataRoot = ""
mongoUri = ""
mongoDb = ""


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


def _adler32(filepath):  # Does this work for large files?
    with open(filepath, "rb") as file:
        adler32_value = adler32(file.read())
    return hex(adler32_value)[2:]


def _sha1(filepath):
    sha1_value = sha1()
    with open(filepath, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            sha1_value.update(chunk)
    return sha1_value.hexdigest()


def main(configfile='/etc/dcache/container.conf'):
    # global variables
    global running
    # are they needed?
    global archiveUser
    global archiveMode
    global mountPoint
    global dataRoot
    global mongoUri
    global mongoDb

    # general variables
    checksum_calculation = {"md5": _md5,
                            "adler32": _adler32,
                            "sha1": _sha1
                            }
    logger = logging.getLogger()
    log_handler = None
    # Make this configurable (via MongoDB)
    osm_type = "osm"
    osm_name = "osm"

    while running:
        # Read configuration
        configuration = parser.RawConfigParser(defaults={'scriptId': 'pack', 'mongoUri': 'mongodb://localhost/',
                                                         'mongoDb': 'smallfiles', 'logLevel': 'ERROR'})

        # Configure parameters from configuration file
        try:
            configuration.read(configfile)
            script_id = configuration.get('DEFAULT', 'script_id')
            log_level_str = configuration.get('DEFAULT', 'log_level')
            archiveUser = configuration.get('DEFAULT', 'archiveUser')
            archiveMode = configuration.get('DEFAULT', 'archiveMode')
            mountPoint = configuration.get('DEFAULT', 'mount_point')
            dataRoot = configuration.get('DEFAULT', 'data_root')
            mongoUri = configuration.get('DEFAULT', 'mongo_url')
            mongoDb = configuration.get('DEFAULT', 'mongo_db')
        except FileNotFoundError as e:
            logger.critical(f'Configuration file "{configfile}" not found. Exiting now.')
            sys.exit(1)
        except parser.NoSectionError as e:
            # Section DEFAULT nicht gefunden
            logger.critical(f'Section [DEFAULT] was not found in "{configfile}". This section is mandatory, exiting now.')
            sys.exit(1)
        except parser.NoOptionError as e:
            # Option (e.g. data_root) not found in section
            logger.critical(f'An option is missing in section [DEFAULT] of file "{configfile}", exiting now: {e}')
            sys.exit(1)
        except parser.MissingSectionHeaderError as e:
            # file doesn't contain section header
            logger.critical(f'The file "{configfile}" doesn\'t contain section headers. Exiting now')
            sys.exit(1)
        except parser.ParsingError as e:
            # error while parsing
            logger.critical(f'There was an error parsing while parsing the configuration "{configfile}", exiting now: {e}')
            sys.exit(1)
        except parser.Error as e:
            # Base class of other configparser exceptions, maybe delete this
            logger.critical(f'An error occurred while reading the configuration file {configfile}, exiting now: {e}')
            sys.exit(1)

        log_level = getattr(logging, log_level_str.upper(), None)
        logger.setLevel(log_level)

        if log_handler is not None:
            log_handler.close()
            logger.removeHandler(log_handler)

        log_handler = logging.handlers.WatchedFileHandler(f'/var/log/dcache/writebfids-{script_id}.log')
        formatter = logging.Formatter('%(asctime)s %(name)-10s %(levelname)-8s %(message)s')
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)

        logging.info(f'Successfully read configuration from file {configfile}.')

        # Connect to database and get archives
        client = MongoClient(mongoUri)
        db = client[mongoDb]
        with db.archives.find() as db_archives:
            logger.info(f"Found {db.archives.count_documents({})} new archives")
            for archive in db_archives:
                if not running:
                    logger.info("Exiting")
                    sys.exit(0)
                # Open ZIP-File and get filelist
                logger.info(f"Processing archive {archive['path']}")
                try:
                    zip_file = ZipFile(archive['path'], mode="r", allowZip64=True)
                    archive_pnfsidlist = [f.filename for f in zip_file.filelist]
                except BadZipfile:
                    logger.warning(f"Archive {archive['path']} is not ready yet. Will try again later.")
                    continue
                except FileNotFoundError:
                    logger.error(f"Container {archive['path']} could not be found on local disk. Files, that should be "
                                 f"in this archive, are now reset to be packed again.")
                    for pnfsid in archive_pnfsidlist:
                        file_result = db.files.find_one({"pnfsid": pnfsid})
                        file_result['state'] = "new"
                        db.files.replace_one({"pnfsid", pnfsid}, file_result)
                        logger.debug(f"Resetted file with PNFSID {pnfsid}")
                finally:
                    zip_file.close()

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
                        db_file = db.files.find_one({"pnfsid": pnfsid})
                        db_file['state'] = "new"
                        db.files.replace_one({"pnfsid": pnfsid}, db_file)

                # Upload zip-file to dCache
                auth = HTTPBasicAuth('admin', 'dickerelch')  # TODO make it configurable
                url = f"https://localhost:2881/archives/{os.path.basename(archive['path'])}"  # TODO make it configurable
                headers = {"Content-type": "application/octet-stream"}
                retry_counter = 0
                response_status_code = 0
                while retry_counter <= 3 and response_status_code not in (200, 201):
                    try:
                        response = requests.put(url, data=open(archive['path'], 'rb'), verify=False, auth=auth, headers=headers)
                    except Exception as e:
                        logger.error(f"An exception occured while uploading zip-file to dCache. Will retry in a "
                                     f"few seconds: {e}")
                        retry_counter += 1
                        time.sleep(10)
                        continue
                    response_status_code = response.status_code
                    logger.debug(f"Uploading zip-file finished with status code {response_status_code}")
                    if response_status_code not in (200, 201):
                        logger.info(f"Uploading file to dCache failed as the returned status code, "
                                    f"{response_status_code}, is not 200 or 201. Retrying in a few seconds.")
                        retry_counter += 1
                        time.sleep(10)
                if retry_counter == 4:
                    logger.critical(f"Zip-file could not be uploaded to dCache, even after retrying "
                                    f"{retry_counter - 1} time(s). Please check your dCache! Exiting script now...")
                    sys.exit(1)

                # Request PNFSID and Checksum from dCache, calculate local checksum
                headers = {"Want-Digest": "ADLER32,MD5,SHA1"}
                retry_counter = 0
                response_status_code = 0
                while retry_counter <= 3 and response_status_code not in (200, 201):
                    try:
                        response = requests.head(url, verify=False, auth=auth, headers=headers)
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
                    logger.critical(f"Checksum and pnfsid of zip-file could not be requested from dCache, even after "
                                    f"retrying {retry_counter - 1} time(s). Please chack your dCache! Exiting script "
                                    f"now...")
                    sys.exit(1)

                pnfsid = response.headers.get("ETag").split('_')[0].replace('"', '')
                checksum_type, remote_checksum = response.headers.get("Digest").split('=', 1)
                if checksum_type not in checksum_calculation.keys():
                    logger.error(f"Checksum type {checksum_type} is not implemented!")
                    raise NotImplementedError()
                local_checksum = checksum_calculation[checksum_type](archive['path'])

                # Compare Checksums
                count_updated = 0
                logger.debug(f"Checksum of archive locally: {local_checksum}, on dCache: {remote_checksum}; "
                             f"Checksum type: {checksum_type}")
                if remote_checksum == local_checksum:
                    for file_pnfsid in archive_pnfsidlist:
                        file_entry = db.files.find_one({'pnfsid': file_pnfsid})
                        if file_entry is None:
                            continue
                        archive_url = f"{osm_type}://{osm_name}/?store={file_entry['store']}&group=" \
                                      f"{file_entry['group']}&bfid={file_pnfsid}:{pnfsid}"
                        file_entry['archiveUrl'] = archive_url
                        file_entry['state'] = f"verified: {archive['path']}"
                        db.files.replace_one({"pnfsid": file_pnfsid}, file_entry)
                        count_updated += 1
                        logger.debug(f"Updated file with pnfsid {file_pnfsid}")
                    logger.info(f"Updated {count_updated} file records in MongoDB")
                else:
                    logger.error(f"Checksums of local and remote zip-file didn't match. Going to delete archive to "
                                 f"reupload it next run.")
                    # delete file on dCache
                    auth = HTTPBasicAuth('admin', 'dickerelch')  # TODO make it configurable
                    url = f"https://localhost:2881/archives/{os.path.basename(archive['path'])}"  # TODO make it configurable
                    response = requests.delete(url, auth=auth, verify=False)
                    if response.status_code == 204:
                        logger.info(f"Archive was successfully deleted from dCache.")
                    else:
                        logger.info(f"Archive wasn't deleted from dCache, status code: {response.status_code}")
                        continue

                # Cleanup
                os.remove(archive['path'])
                logger.debug(f"Deleted local archive {archive['path']}")
                db.archives.delete_one({"path": archive['path']})
                logger.debug(f"Removed MongoDB record in archives for archive {archive['path']}")
                logger.info(f"Finished processing {archive['path']}")
            if client is not None:
                client.close()
            time.sleep(60)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)
    sys.excepthook = uncaught_handler
    if not os.getuid() == 0:
        print("writebfids.py must run as root!")
        sys.exit(2)

    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print("Usage: writebfids.py <configfile>")
        sys.exit(2)

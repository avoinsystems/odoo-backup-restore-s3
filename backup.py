#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
from datetime import datetime
from xmlrpc import client

import argparse
import os
import sys
import time

from boto3.session import Session
import logging
from logging.config import dictConfig

from io import BytesIO

logging_config = dict(
    version=1,
    formatters={
        'f': {'format': '%(levelname)-8s %(message)s'}
    },
    handlers={
        'h': {'class': 'logging.StreamHandler',
              'formatter': 'f',
              'level': logging.INFO}
    },
    root={
        'handlers': ['h'],
        'level': logging.INFO,
    },
)

dictConfig(logging_config)
_logger = logging.getLogger()


def backup(databases, odoo_host, odoo_port, odoo_master_password, odoo_version,
           aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket,
           s3_path, **kwargs):
    databases_to_backup = databases.split(',')

    # Create the XML RPC connection
    uri = 'http://{}:{}/xmlrpc/db'.format(odoo_host, odoo_port)
    conn = client.ServerProxy(uri)

    # Get the database list
    db_list = conn.list()

    # Create the AWS S3 connection
    aws_conn = Session(aws_access_key_id, aws_secret_access_key,
                       region_name=aws_region)
    s3 = aws_conn.resource('s3')

    dbs_not_found = set(databases_to_backup) - set(db_list)
    if dbs_not_found:
        raise Exception(
            "Unable to perform backup. "
            "Database(s) {} can't be found.".format(dbs_not_found))

    # Iterate through the databases to backup
    for database in databases_to_backup:
        filename = "{}_{}.zip".format(database,
                                      time.strftime('%Y-%m-%d_%H-%M-%S'))

        # Download the backup dump from Odoo
        if odoo_version == '8':
            data = conn.dump(odoo_master_password, database)
        else:  # 9 and 10
            data = conn.dump(odoo_master_password, database, 'zip')

        data = base64.b64decode(data)

        _logger.info(u"Successfully dumped database '{}'. Uploading to S3 ..."
                     .format(database))

        # Upload the dump to S3
        upload_path = s3_path + '/' + filename
        s3.Bucket(s3_bucket).put_object(Key=upload_path, Body=data)
        _logger.info(u"Upload to S3 finished. Database '{}' dump saved as {}"
                     .format(database, filename))


def restore(databases, odoo_host, odoo_port, odoo_master_password,
           aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket,
           s3_path, restore_filename, **kwargs):

    database_to_restore = databases.split(',')
    assert len(database_to_restore) == 1, 'You can only restore one database ' \
                                          'at once'
    database_to_restore = database_to_restore[0]

    # Add path to restore filename
    restore_key = s3_path + '/' + restore_filename \
        if restore_filename else False

    # Create the XML RPC connection
    uri = 'http://{}:{}/xmlrpc/db'.format(odoo_host, odoo_port)
    conn = client.ServerProxy(uri)

    # Get the database list
    db_list = conn.list()

    # Create the AWS S3 connection
    aws_conn = Session(aws_access_key_id, aws_secret_access_key,
                       region_name=aws_region)
    s3 = aws_conn.resource('s3')

    if database_to_restore in db_list:
        raise Exception(
            "Unable to perform restore. "
            "Database '{}' already exists.".format(database_to_restore))

    # Get a list of the backup files in the path
    bucket = s3.Bucket(s3_bucket)
    backup_files = bucket.objects.filter(Prefix=s3_path)
    backup_filenames = [file.key for file in backup_files]

    if restore_key and restore_key not in backup_filenames:
        raise FileNotFoundError(
            'Backup file {} not found in S3 bucket {}.'
            .format(restore_key, s3_bucket))
    elif not restore_key:
        # If the filename is not specified, find the latest dump
        latest = False

        for iter_file in backup_files:
            if not latest or iter_file.last_modified > latest:
                latest = iter_file.last_modified
                restore_key = iter_file.key

        if database_to_restore not in restore_key:
            _logger.warning("Latest dump key is {} but it doesn't "
                            "contain the database name '{}'."
                            .format(restore_key, database_to_restore))

    # Download the backup from S3
    _logger.info('Downloading {} from S3 ...'.format(restore_key))
    file = BytesIO()
    bucket.download_fileobj(restore_key, file)
    data = base64.encodebytes(file.getvalue()).decode('utf-8')

    _logger.info('Successfully downloaded {} from S3. Restoring dump '
                 'to database {}'.format(restore_key,
                                         database_to_restore))

    # Restore the backup
    conn.restore(odoo_master_password, database_to_restore, data)

    _logger.info(u"Successfully restored {} to database '{}'."
                 .format(restore_key, database_to_restore))


if __name__ == "__main__":

    env = os.environ

    parser = argparse.ArgumentParser()
    parser.add_argument('--databases', default=env['DATABASES'])
    parser.add_argument('--odoo-host', default=env['ODOO_HOST'])
    parser.add_argument('--odoo-port', default=env['ODOO_PORT'])
    parser.add_argument('--odoo-master-password',
                        default=env['ODOO_MASTER_PASSWORD'])
    parser.add_argument('--odoo-version', default=env['ODOO_VERSION'])
    parser.add_argument('--aws-access-key-id',
                        default=env['AWS_ACCESS_KEY_ID'])
    parser.add_argument('--aws-secret-access-key',
                        default=env['AWS_SECRET_ACCESS_KEY'])
    parser.add_argument('--aws-region', default=env['AWS_REGION'])
    parser.add_argument('--s3-bucket', default=env['S3_BUCKET'])
    parser.add_argument('--s3-path', default=env['S3_PATH'])
    parser.add_argument('--check-url', default=env['CHECK_URL'])
    parser.add_argument('--restore-filename', default=env['RESTORE_FILENAME'])
    parser.add_argument('mode', default='backup')
    args = parser.parse_args()

    supported_versions = ['8', '9', '10']
    if args.odoo_version not in supported_versions:
        _logger.error('Invalid Odoo version {}. Supported versions: {}'
                      .format(args['odoo_version'], supported_versions))
        sys.exit(1)

    if args.mode == 'restore':
        restore(**vars(args))
    elif args.mode == 'backup':
        backup(**vars(args))

        if args.check_url:
            import requests

            requests.get(args.check_url)
    else:
        _logger.error('Invalid mode: {}.'.format(args.mode))

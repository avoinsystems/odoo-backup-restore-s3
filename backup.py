#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
from datetime import datetime
from xmlrpc import client

import configargparse
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

actions = dict()

def main(args):
    # Create the AWS S3 connection
    aws_conn = Session(
        args['aws_access_key_id'],
        args['aws_secret_access_key'],
        region_name=args['aws_region']
    )
    args['s3'] = aws_conn.resource('s3')
    if args['protocol'] == 'xmlrpc':
        args['conn'] = client.ServerProxy(
            'http://{odoo_host}:{odoo_port}/xmlrpc/db'.format(**args)
        )
    action_name = args['mode'] + '_' + args['protocol']
    return actions[action_name](**args)


def backup_xmlrpc(s3, conn, databases, odoo_host, odoo_port, odoo_master_password, odoo_version,
           aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket,
           s3_path, **kwargs):

    # Get the database list
    db_list = conn.list()
    dbs_not_found = set(databases) - set(db_list)
    if dbs_not_found:
        raise Exception(
            "Unable to perform backup. "
            "Database(s) {} can't be found.".format(dbs_not_found))

    # Iterate through the databases to backup
    for database in databases:
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

actions['backup_xmlrpc'] = backup_xmlrpc


def backup_http(s3, databases, odoo_host, odoo_port, odoo_master_password, odoo_version,
           aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket,
           s3_path, **kwargs):
    import requests

    # Iterate through the databases to backup
    backup_url = "http://{}:{}/web/database/backup".format(odoo_host, odoo_port)
    request_data = dict(
        master_pwd=odoo_master_password,
        backup_format='zip'
    )
    request_args = dict(
        url=backup_url,
        stream=True,
        data=request_data
    )
    for database in databases:
        filename = "{}_{}.zip".format(database,
                                      time.strftime('%Y-%m-%d_%H-%M-%S'))
        request_data['name'] = database
        # Download the backup dump from Odoo
        response = requests.post(**request_args)

        if response.status_code >= 400:
            raise Exception("Odoo returned error {} when trying to backup database {}.".format(
                response.status_code,
                database
            ))

        _logger.info(u"Successfully dumped database '{}'. Uploading to S3 ..."
                     .format(database))

        # Upload the dump to S3
        upload_path = s3_path + '/' + filename
        s3.Bucket(s3_bucket).upload_fileobj(response.raw, Key=upload_path)
        _logger.info(u"Upload to S3 finished. Database '{}' dump saved as {}"
                     .format(database, filename))

actions['backup_http'] = backup_http


def restore_xmlrpc(conn, s3, databases, odoo_host, odoo_port, odoo_master_password,
           aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket,
           s3_path, restore_filename, **kwargs):

    assert len(databases) == 1, 'You can only restore one database ' \
                                          'at once'
    database = databases[0]

    # Add path to restore filename
    restore_key = s3_path + '/' + restore_filename \
        if restore_filename else False

    # Get the database list
    db_list = conn.list()

    if database in db_list:
        raise Exception(
            "Unable to perform restore. "
            "Database '{}' already exists.".format(database))

    # Get a list of the backup files in the path
    bucket = s3.Bucket(s3_bucket)
    restore_key = check_and_fix_restore_key(bucket, database, restore_key, s3_bucket, s3_path)

    # Download the backup from S3
    _logger.info('Downloading {} from S3 ...'.format(restore_key))
    file = BytesIO()
    bucket.download_fileobj(restore_key, file)
    data = base64.encodebytes(file.getvalue()).decode('utf-8')

    _logger.info('Successfully downloaded {} from S3. Restoring dump '
                 'to database {}'.format(restore_key,
                                         database))

    # Restore the backup
    conn.restore(odoo_master_password, database, data)

    _logger.info(u"Successfully restored {} to database '{}'."
                 .format(restore_key, database))

actions['restore_xmlrpc'] = restore_xmlrpc


def restore_http(s3, databases, odoo_host, odoo_port, odoo_master_password,
           aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket,
           s3_path, restore_filename, **kwargs):
    import requests
    import tempfile
    assert len(databases) == 1, 'You can only restore one database ' \
                                          'at once'
    database = databases[0]

    # Add path to restore filename
    restore_key = s3_path + '/' + restore_filename \
        if restore_filename else False

    base_url = 'http://{}:{}/web/database/'.format(odoo_host, odoo_port)

    # Get the database list
    response = requests.post(
        base_url + 'list',
        headers={'content-type': 'application/json'},
        data='{}'
    )

    db_list = response.json()['result']

    if database in db_list:
        raise Exception(
            "Unable to perform restore. "
            "Database '{}' already exists.".format(database))

    # Get a list of the backup files in the path
    bucket = s3.Bucket(s3_bucket)
    restore_key = check_and_fix_restore_key(bucket, database, restore_key, s3_bucket, s3_path)

    _logger.info('Downloading {} from S3 ...'.format(restore_key))

    with tempfile.TemporaryFile() as file:
        bucket.download_fileobj(restore_key, file)
        file.seek(0)

        _logger.info('Successfully downloaded {} from S3. Restoring dump '
                 'to database {}'.format(restore_key,
                                         database))

        # Post the file to Odoo
        response = requests.post(
            base_url + 'restore',
            files=dict(backup_file=('s3_db.zip', file, 'application/zip')),
            data=dict(master_pwd=odoo_master_password, name=database),
        )

    # Ugly af, I know.
    if response.status_code >= 400 or 'Database restore error:' in response.text:
        if 'Database restore error:' in response.text:
            text = response.text.split("Database restore error:", 1)[1].split("\n", 1)[0]
        else:
            text = response.text
        raise Exception("There was an error restoring the database to Odoo.\n{}".format(text))

    _logger.info(u"Successfully restored {} to database '{}'."
                 .format(restore_key, database))


def check_and_fix_restore_key(bucket, database, restore_key, s3_bucket, s3_path):
    backup_files = bucket.objects.filter(Prefix=s3_path)
    if restore_key and all(restore_key != file.key for file in backup_files):
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

        if database not in restore_key:
            _logger.warning("Latest dump key is {} but it doesn't "
                            "contain the database name '{}'."
                            .format(restore_key, database))

    return restore_key


actions['restore_http'] = restore_http



if __name__ == "__main__":

    env = os.environ
    supported_versions = ('8', '9', '10')

    parser = configargparse.ArgParser()
    parser.add_argument('mode', default='backup', choices=('backup', 'restore'))
    parser.add_argument('-c', '--config', is_config_file=True, help='Path to configuration file. Either .ini or .yaml syntax accepted')
    parser.add_argument('--databases', env_var='DATABASES', required=True, type=lambda s: s.split(','))
    parser.add_argument('--odoo-host', env_var='ODOO_HOST', default='odoo')
    parser.add_argument('--odoo-port', env_var='ODOO_PORT', default=8069)
    parser.add_argument('--odoo-master-password', env_var='ODOO_MASTER_PASSWORD', default='admin')
    parser.add_argument('--odoo-version', env_var='ODOO_VERSION', default=supported_versions[-1], choices=supported_versions)
    parser.add_argument('--aws-access-key-id', env_var='AWS_ACCESS_KEY_ID', required=True)
    parser.add_argument('--aws-secret-access-key', env_var='AWS_SECRET_ACCESS_KEY', required=True)
    parser.add_argument('--aws-region', env_var='AWS_REGION', required=True)
    parser.add_argument('--s3-bucket', env_var='S3_BUCKET', required=True)
    parser.add_argument('--s3-path', env_var='S3_PATH', default='backup')
    parser.add_argument('--check-url', env_var='CHECK_URL', help="After every backup, send an HTTP GET request to this address. Designed with healthchecks.io in mind.")
    parser.add_argument('--restore-filename', env_var='RESTORE_FILENAME')
    parser.add_argument('--protocol', env_var='PROTOCOL', default='xmlrpc', choices=('xmlprc', 'http'))

    args = parser.parse_args()

    main(vars(args))

    if args.check_url:
        import requests

        requests.get(args.check_url)

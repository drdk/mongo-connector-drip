# Copyright 2013-2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
the defined HTTP endpoint.
"""
import base64
import logging
import httplib
import json
import util
import time
import os
import pymongo

from datetime import datetime
from threading import Timer
from bson import json_util
from iron_mq import *

from mongo_connector import errors
from mongo_connector.compat import u
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

wrap_exceptions = exception_wrapper({
    pymongo.errors.ConnectionFailure: errors.ConnectionFailed,
    pymongo.errors.OperationFailure: errors.OperationFailed})

LOG = logging.getLogger(__name__)

class DateTimeDocumentFormatter(DefaultDocumentFormatter):

    def transform_value(self, value):
        if isinstance(value, datetime):
            iso = value.isoformat()
            return iso[:-3] + 'Z'
        else:
            return super(DateTimeDocumentFormatter, self).transform_value(value)

class DocManager(DocManagerBase):
    """Implementation of the DocManager interface.
    Receives documents from an OplogThread and sends updates to Endpoint.
    """

    def __init__(self, url, chunk_size, auto_commit_interval=DEFAULT_COMMIT_INTERVAL, unique_key='_id', **kwargs):
        self.ironmq = IronMQ(host=kwargs.pop('ironMqHost', None),
                project_id=kwargs.pop('ironMqProjectId', None),
                token=kwargs.pop('ironMqToken', None))
        self.ironMqQueue = self.ironmq.queue(kwargs.pop('ironMqQueueName', None))
        try:
            self.mongo = pymongo.MongoClient(
            kwargs.get('mongoUrl'))
            self.mongo.MediaUniverse.authenticate(kwargs.pop('auth_username', None), kwargs.pop('auth_key', None))
        except pymongo.errors.InvalidURI:
            raise errors.ConnectionFailed("Invalid URI for MongoDB")
        except pymongo.errors.ConnectionFailure:
            raise errors.ConnectionFailed("Failed to connect to MongoDB")
        self.unique_key = unique_key        
        self.auto_commit_interval = auto_commit_interval
        self.chunk_size = chunk_size
        self._formatter = DateTimeDocumentFormatter()

        self.has_attachment_mapping = False

    def stop(self):
        """Stop the auto-commit thread."""
        self.auto_commit_interval = None

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        db, coll = self._db_and_collection(namespace)
        message = self.mongo[db][coll].find_one({"_id" : document_id})
        messages = [message]
        self.bulk_upsert(messages, namespace, timestamp, 'U')

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp):
        messages = [doc]
        self.bulk_upsert(messages, namespace, timestamp)

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp, action='C'):
        jsondocs = (self._doc_to_json(self._formatter.format_document(d), str(d[self.unique_key]), action, timestamp) for d in docs)
        if self.chunk_size > 0:
            batch = list(next(jsondocs) for i in range(self.chunk_size))
            while batch:
                messages = []
                messages.extend(batch)
                if (self._send_upsert(messages)):
                    self.write_last_doc_timestamp(timestamp)
                else:
                    break
                batch = list(next(jsondocs) for i in range(self.chunk_size))
        else:
            if (self._send_upsert(jsondocs)):
                self.write_last_doc_timestamp(timestamp)

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        message = self._doc_to_json(None, str(document_id), 'D', timestamp)
        messages = [message]
        if (self._send_upsert(messages)):
            self.write_last_doc_timestamp(timestamp)

    def write_last_doc_timestamp(self, timestamp):
        with open('generic_http_doc_manager_maxtouched', 'w') as progress_file:
            data = []
            data.append(str(timestamp))
            progress_file.writelines(data)

    def commit(self):
        pass

    def search(self, start_ts, end_ts):
        pass
    
    def handle_command(self, command_doc, namespace, timestamp):
        pass

    @wrap_exceptions
    def get_last_doc(self):
        """Get the most recently modified document timestamp from endpoint.
        """
        if not os.path.exists('generic_http_doc_manager_maxtouched'):
            try:
                # Create maxtouched progress file
                open('generic_http_doc_manager_maxtouched', "w").close()
            except IOError as e:
                LOG.critical("MongoConnector: Could not "
                            "create a maxtouched log: %s" %
                            str(e))
                sys.exit(2)

        with open('generic_http_doc_manager_maxtouched', 'r') as progress_file:
            try:
                timestamp = progress_file.readline();
            except ValueError:
                LOG.exception('Cannot read generic http doc manager max touched file')
                return
        
        if not timestamp or timestamp == -1:
            return None
        else:
            dict = []
            newitem = {'_ts' : int(timestamp)}
            dict.append(newitem)
            return dict

    def _db_and_collection(self, namespace):
        return namespace.split('.', 1)

    def _doc_to_json(self, doc, document_id, action, timestamp):
        message = {
        'action' : action,
        '_ts' : timestamp,
        '_id' : document_id,
        'body' : doc
        }
        return message

    def _send_upsert(self, docData):
        success = True
        for element in docData:
          jsonOut = json.dumps(element, default=json_util.default)
          self.ironMqQueue.post(jsonOut)
        return success


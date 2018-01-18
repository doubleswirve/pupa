import os
import json
from collections import OrderedDict
from datetime import datetime, timezone

from pupa import utils

from google.oauth2 import service_account
from google.cloud import pubsub


class GoogleCloudPubSub():

    def __init__(self, caller):
        self._connect()
        self.topic_path = self.publisher.topic_path(
            os.environ.get('GOOGLE_CLOUD_PROJECT'),
            os.environ.get('GOOGLE_CLOUD_PUBSUB_TOPIC'))
        self.caller = caller

    def _connect(self):
        # Allow users to explicitly provide service account info (i.e.,
        # stringified JSON) or, if on Google Cloud Platform, allow the chance
        # for credentials to be detected automatically
        #
        # @see http://google-cloud-python.readthedocs.io/en/latest/pubsub/index.html
        service_account_data = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')
        if service_account_data:
            # @see https://github.com/GoogleCloudPlatform/google-auth-library-python/issues/225
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(service_account_data),
                scopes=('https://www.googleapis.com/auth/pubsub',))
            self.publisher = pubsub.PublisherClient(credentials=credentials)
        else:
            self.publisher = pubsub.PublisherClient()

    def _publish(self, message, retry_attempt=0, retry_max_attempts=3):
        try:
            self.publisher.publish(
                self.topic_path,
                message,
                pubdate=datetime.now(timezone.utc).strftime('%c'))
        except Exception as publishing_exception:
            # Workaround attempt for `StatusCode.UNAUTHENTICATED` error
            # we've been receiving for scrapes that take a longer time
            #
            # @see https://github.com/GoogleCloudPlatform/google-cloud-python/issues/3212
            if retry_attempt < retry_max_attempts:
                # Try to reconnect and attempt publishing again
                self._connect()
                self._publish(message, retry_attempt + 1, retry_max_attempts)
            else:
                self.error(publishing_exception)

    def save_object(self, obj):
        obj.pre_save(self.caller.jurisdiction.jurisdiction_id)

        self.caller.info('save %s %s to topic %s', obj._type, obj, self.topic_path)
        self.caller.debug(json.dumps(OrderedDict(sorted(obj.as_dict().items())),
                                     cls=utils.JSONEncoderPlus,
                                     indent=4, separators=(',', ': ')))

        self.caller.output_names[obj._type].add(obj)

        # Copy the original object so we can tack on jurisdiction and type
        output_obj = obj.as_dict()

        if self.caller.jurisdiction:
            output_obj['jurisdiction'] = self.caller.jurisdiction.jurisdiction_id

        output_obj['type'] = obj._type

        # TODO: Should add a messagepack CLI option
        message = json.dumps(output_obj,
                             cls=utils.JSONEncoderPlus,
                             separators=(',', ':')).encode('utf-8')

        self._publish(message)

        # validate after writing, allows for inspection on failure
        try:
            # Note we're validating the original object, not the output object,
            # Because we add some relevant-to-us but out of schema metadata to the output object
            obj.validate()
        except Exception as ve:
            if self.caller.strict_validation:
                raise ve
            else:
                self.caller.warning(ve)

        # after saving and validating, save subordinate objects
        for obj in obj._related:
            self.save_object(obj)

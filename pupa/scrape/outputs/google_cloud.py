import http.client
import os
import json
from collections import OrderedDict
from datetime import datetime, timezone

from pupa import utils

from google.oauth2 import service_account
from google.cloud import pubsub


class GoogleCloudPubSub():

    def __init__(self, caller):
        # HACK: SC workaround
        self._http_version_hack_init()
        self._http_version_hack_set()

        # Allow users to explicitly provide service account info (i.e.,
        # stringified JSON) or, if on Google Cloud Platform, allow the chance
        # for credentials to be detected automatically
        #
        # @see http://google-cloud-python.readthedocs.io/en/latest/pubsub/index.html
        service_account_data = os.environ.get('GOOGLE_CLOUD_PUBSUB_CREDENTIALS')
        if service_account_data:
            # @see https://github.com/GoogleCloudPlatform/google-auth-library-python/issues/225
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(service_account_data),
                scopes=('https://www.googleapis.com/auth/pubsub',))
            self.publisher = pubsub.PublisherClient(credentials=credentials)
        else:
            self.publisher = pubsub.PublisherClient()

        self.topic_path = self.publisher.topic_path(
            os.environ.get('GOOGLE_CLOUD_PROJECT'),
            os.environ.get('GOOGLE_CLOUD_PUBSUB_TOPIC'))

        self.caller = caller

        # HACK: SC...
        self._http_version_hack_undo()

    def save_object(self, obj):
        # HACK: SC...
        self._http_version_hack_set()

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

        self.publisher.publish(
            self.topic_path,
            message,
            pubdate=datetime.now(timezone.utc).strftime('%c'))

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

        # HACK: SC...
        self._http_version_hack_undo()

        # after saving and validating, save subordinate objects
        for obj in obj._related:
            self.save_object(obj)

    def _http_version_hack_init(self):
        """Cache currently set versions of HTTP in instance vars
        """

        self._http_version = http.client.HTTPConnection._http_vsn
        self._http_version_string = http.client.HTTPConnection._http_vsn_str

    def _http_version_hack_set(self):
        """If necessary, temporarily update HTTP version for Google
        """

        if self._http_version < 11:
            http.client.HTTPConnection._http_vsn = 11
            http.client.HTTPConnection._http_vsn_str = 'HTTP/1.1'

    def _http_version_hack_undo(self):
        """Reset to original versions
        """

        http.client.HTTPConnection._http_vsn = self._http_version
        http.client.HTTPConnection._http_vsn_str = self._http_version_string

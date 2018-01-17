import os
import json
import redis
from collections import OrderedDict

from pupa import utils


class RedisPubSub():

    def __init__(self, caller):
        self.client = redis.StrictRedis(
            host=os.environ.get('REDIS_HOST', 'localhost'),
            port=os.environ.get('REDIS_PORT', 6379),
            db=os.environ.get('REDIS_DB', 0),
            password=os.environ.get('REDIS_PASSWORD', None))
        self.channel = os.environ.get('REDIS_PUBSUB_CHANNEL')
        self.caller = caller

    def save_object(self, obj):
        obj.pre_save(self.caller.jurisdiction.jurisdiction_id)

        self.caller.info('save %s %s to channel %s', obj._type, obj, self.channel)
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

        self.client.publish(self.channel, message)

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

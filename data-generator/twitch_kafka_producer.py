#!/usr/bin/env python
import datetime
import twitch
from itertools import islice
import json
from confluent_kafka import Producer

client = twitch.TwitchHelix(client_id=<client_id>,
                            client_secret=<client_secret>,
                            scopes=[twitch.constants.OAUTH_SCOPE_ANALYTICS_READ_EXTENSIONS])
client.get_oauth()

# Producer instance
p = Producer({'bootstrap.servers': 'kafka:29092'})


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))


def json_serializer(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise "Type %s not serializable" % type(obj)


try:
    streams = client.get_streams(page_size=100)

    while streams.next:

        for stream in islice(streams, 0, 100):
            p.produce('twitch-streams', json.dumps(stream, default=json_serializer, ensure_ascii=False).encode('utf-8'), callback=acked)
            p.poll(0.5)

except Exception, e:
    print >> sys.stderr, "Exception: %s" % str(e)
    sys.exit(1)

p.flush(30)

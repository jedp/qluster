import redis
import uuid
import job

class Queue(object):
    def __init__(self):
        self.client = redis.Redis()

    def create(self, kind, data):
        return job.Job(uuid.uuid4(), kind, data)

class Supervisor(object):
    def __init__(self):
        self.queue = Queue()


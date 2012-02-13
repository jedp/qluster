import sys
import datetime
import dateutil.tz
import time
import random
import redis
import redis.exceptions
import uuid

PRIORITIES = {
    'low': 30,
    'normal': 20,
    'high': 10,
    'critical': 0}

ONE_SECOND = 1
ONE_MINUTE = 60
ONE_HOUR = ONE_MINUTE * 60

# ----------------------------------------------------------------------
# exceptions

class JobRemoved(Exception): 
    pass

class JobLocked(Exception): 
    def __init__(self, message, expires=0):
        Exception.__init__(self, message)
        self.expires = expires

class JobDoesNotExist(Exception): 
    pass

# ----------------------------------------------------------------------
# utils

def getCurrentTime():
    return datetime.datetime.now(dateutil.tz.tzlocal())

# ----------------------------------------------------------------------
# Job object
# to create a job that doesn't exist, provide 'kind' and 'data' keywords.
# to claim an existing job atomically, provide job_id
#
# or maybe just use these instead:
#
#   createJob(kind, data)
#   claimJob(job_id)

class Job(object):
    def __init__(self, job_id, **params):
        self.client = redis.Redis(port = params.get('port', 6379))
        self.removed = False

        # default data
        self.params = {
            'created_at': None,
            'updated_at': None,
            'failed_at': None,
            'error': None,
            'priority': PRIORITIES[params.get('priority', 'normal')],
            'state': 'inactive',
            'timeout': params.get('timeout', ONE_MINUTE),
            'data': None}

        self.expiration = 0
        self.job_id = job_id
        self.claim()
        self.load()

    def _checkTimeout(fn):
        """
        If our internal timer has run out, the job has expired
        and it's possible another worker has claimed it.  Try
        to re-claim the job.

        Raise a JobLocked exception on failure
        """
        def _dec(this, *args, **kw):
            if 0 > this.expiration - time.time():
                try:
                    this.claim()
                except JobLocked:
                    raise JobLocked("Job timed out and claimed by another worker")

            # We still have the job, so execute the function
            return fn(this, *args, **kw)
        return _dec

    def _markUpdated(fn):
        """
        Update the modification time and creation time if not set
        """
        def _dec(this, *args, **kw):
            now = getCurrentTime()
            if this.params['created_at'] is None:
                this.client.sadd('q:kinds', this.params['kind'])
                this.params['created_at'] = now
            this.params['updated_at'] = now
            return fn(this, *args, **kw)
        return _dec

    def timeToLive(self):
        return max(0, self.expiration - time.time())

    def load(self):
        """
        Load the job data for thie job_id from redis
        """
        if not self.client.exists('q:job:%s' % (self.job_id)):
            raise JobDoesNotExist(self.job_id)
        for k, v in self.client.hgetall('q:job:%s' % (self.job_id)).items():
            self.params[k] = v

    @_checkTimeout
    @_markUpdated
    def save(self):
        """
        Save the job data and push the timeout back
        """
        # Update the data
        self.client.hmset('q:job:' + self.job_id, self.params)

        # Update the job expiration 
        expires = time.time() + float(self.get('timeout'))
        self.client.set('q:job:%s:expires' % (self.job_id), expires)
        self.expiration = expires

    def remove(self):
        """
        Remove the job 
        """
        self._clearState()
        self.client.delete('q:job:' + self.job_id)
        self.client.zrem('q:jobs', self.job_id)
        self.client.delete('q:job:%s:expires' % (self.job_id))
        self.expiration = 0
        self.removed = True

    def set(self, key, value):
        """
        Set a single value in the job's data hash and save to redis
        """
        if self.removed:
            raise JobRemoved("Cannot write to removed job")

        if key == 'state':
            self.setState('value')
        else:
            self.client.hset('q:job:' + self.job_id, key, value)
            self.params[key] = value
        self.save()

    def get(self, key):
        """
        Get a value from the job's data hash by its key
        """
        return self.params.get(key)

    def _clearState(self):
        """
        Clear the job's state
        """
        state = self.get('state')
        kind = self.get('kind')
        priority = self.get('priority')
        self.client.zrem('q:jobs:%s' % (state), self.job_id)
        self.client.zrem('q:jobs:%s:%s' % (kind, state), self.job_id)
        
        # as if we called set('state', None)
        self.client.hset('q:job:' + self.job_id, 'state', None)
        self.params['state'] = state

    @_checkTimeout
    def setState(self, state):
        """
        Set the job's state
        """
        self._clearState()
        kind = self.get('kind')
        priority = self.get('priority')
        priority_level = PRIORITIES[priority]
        self.client.zadd('q:jobs', self.job_id, priority_level)
        self.client.zadd('q:jobs:%s' % (state), self.job_id, priority_level)
        self.client.zadd('q:jobs:%s:%s' % (kind, state), self.job_id, priority_level)

        # as if we called set('state', state)
        self.client.hset('q:job:' + self.job_id, 'state', state)
        self.params['state'] = state

    def getState(self):
        return self.get('state')

    state = property(getState, setState)

    def claim(self):
        """
        Try to acquire the job. 

        Returns expiration date on success.
        Raises JobLocked exception on failure.
        """
        # locking protocol from http://redis.io/commands/setnx
        now = time.time()
        expiration = 'q:job:%s:expires' % (self.job_id)
        the_future = now + float(self.get('timeout'))

        # Make sure the job is actually real.
        if not self.client.exists('q:job:' + self.job_id):
            raise JobDoesNotExist("Job not found: %s" % self.job_id)

        # If the expiration has not yet been set, we get the job
        if self.client.setnx(expiration, the_future):
            self.expiration = float(the_future)
            return the_future

        # If the expiration has been set, but it's already expired,
        # we get the job.  Otherwise someone else has locked the job.
        else:
            lock_expired = (0 > float(self.client.get(expiration)) - now)
            if lock_expired:
                current_expiration = float(self.client.getset(expiration, the_future))
                if current_expiration <= the_future:
                    self.expiration = the_future
                    return the_future

            raise JobLocked("Job %s is locked" % (self.job_id))

def createJob(kind, data, pool=((6379, '127.0.0.1'),), assign=None, **kw):
    job_id = str(uuid.uuid4())
    now = time.time()
    priority = kw.get('priority', 'normal')
    state = 'inactive'

    if assign is None:
        assign = random.sample(pool, 1)[0]

    params = {
        'kind': kind,
        'data': data,
        'port': assign[0],
        'host': assign[1],
        'created_at': now,
        'updated_at': now,
        'failed_at': None,
        'error': None,
        'priority': priority,
        'timeout': kw.get('timeout', ONE_MINUTE),
        'state': state}

    priority_level = PRIORITIES[priority]

    # Duplicate job data to all hosts
    for port, host in pool:
        client = redis.Redis(host=host, port=port)
        try:
            client.zadd('q:jobs', job_id, priority_level)
            client.hmset('q:job:' + job_id, params)
        except redis.exceptions.ConnectionError, e:
            print >> sys.stderr, "ERORR:", e

    # Now assign to target
    client = redis.Redis(host=params['host'], port=params['port'])
    client.zadd('q:jobs:%s' % (state), job_id, priority_level)
    client.zadd('q:jobs:%s:%s' % (kind, state), job_id, priority_level)

    return job_id, assign

def claimJob(job_id):
    return Job(job_id)


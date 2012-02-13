import os
import time
import qluster.job
import qluster.test.utils
import redis
from pyvows import Vows, expect

PORTS = (11910, 11911, 11912)
POOL = map(lambda port: (port, '127.0.0.1'), PORTS)

@Vows.create_assertions
def to_be_greater_than(topic, expected):
    return topic > expected

@Vows.create_assertions
def to_be_less_than(topic, expected):
    return topic < expected

@Vows.batch
class WithAClusterOfNodes(Vows.Context):
    def topic(self):
        jid, assign = qluster.job.createJob(
                'harpsichord-tuning', 
                {'scale': 'mean-tone'},
                pool=POOL)
        return jid, assign

    def setup(self):
        self.procs = []
        for port in PORTS:
            self.procs.append(qluster.test.utils.start_redis_server(port))

    def teardown(self):
        for proc in self.procs:
            qluster.test.utils.stop_redis_server( *proc )

    def each_host_knows_all_the_others(self, topic):
        for port, host in POOL:
            client = redis.Redis(port=port, host=host)
            pool = client.smembers('q:pool')
            ports = map(int, [i.split(':')[1] for i in pool])
            expect(ports).to_be_like(PORTS)

    def a_job_is_described_everywhere(self, topic):
        jid, assign = topic
        for port, host in POOL:
            client = redis.Redis(port=port, host=host)
            expect(client.zrange('q:jobs', 0, 1)).to_equal([jid])
            expect(client.hgetall('q:job:' + jid)).Not.to_equal({})
            expect(client.zrange('q:jobs:%s:%d' % (assign[1], assign[0]), 0, -1)).to_equal([jid])

    def a_job_is_only_available_on_the_target(self, topic):
        jid, assign = topic
        for port, host in POOL:
            client = redis.Redis(port=port, host=host)
            if port == assign[0]:
                expect(client.zrange('q:jobs:inactive', 0, 1)).to_equal([jid])
                expect(client.zrange('q:jobs:harpsichord-tuning:inactive', 0, -1)).to_equal([jid])
            else:
                expect(client.zrange('q:jobs:inactive', 0, 1)).to_equal([])
                expect(client.zrange('q:jobs:harpsichord-tuning:inactive', 0, -1)).to_equal([])


    class WhenTheJobIsClaimed(Vows.Context):
        def topic(self, prev):
            jid, assign = prev
            job = qluster.job.Job(jid, assign[0], assign[1])
            return job, assign

        class AfterTheJobIsRemoved(Vows.Context):
            def topic(self, prev):
                job, assign = prev
                job.remove()
                return job, assign

            def it_is_removed_from_all_hosts(self, topic):
                job, assign = topic
                for port, host in POOL:
                    client = redis.Redis(port=port, host=host)
                    expect(client.hgetall('q:job:'+job.job_id)).to_equal({})
                    expect(client.zrange('q:jobs', 0, 1)).to_equal([])
                    expect(client.zrange('q:jobs:%s:%d' % (assign[1], assign[0]), 0, -1)).to_equal([])



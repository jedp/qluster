import os
import time
import qluster.job
import qluster.test.utils
import redis
from pyvows import Vows, expect

PORT = 11900
client = redis.Redis(port=PORT)

@Vows.create_assertions
def to_be_greater_than(topic, expected):
    return topic > expected

@Vows.create_assertions
def to_be_less_than(topic, expected):
    return topic < expected

@Vows.batch
class CreatingAJob(Vows.Context):

    def topic(self):
        jid = qluster.job.createJob(kind='baking', data={'pie': 42}, port=PORT)
        return jid

    def setup(self):
        self.proc, self.conf, self.db = qluster.test.utils.start_redis_server(PORT)

    def teardown(self):
        qluster.test.utils.stop_redis_server(self.proc, self.conf, self.db)

    def creates_a_job_record(self, jid):
        params = client.hgetall('q:job:' + jid)
        expect(params).Not.to_equal(None)
        expect(params['state']).to_equal('inactive')
        expect(params['timeout']).to_equal('60')

    def updates_the_jobs_zset(self, jid):
        expect(client.zrange('q:jobs', 0, -1)).to_equal([jid])

    def updates_the_state_zset(self, jid):
        expect(client.zrange('q:jobs:inactive', 0, -1)).to_equal([jid])

    def updates_the_kind_state_zset(self, jid):
        expect(client.zrange('q:jobs:baking:inactive', 0, -1)).to_equal([jid])

    def leaves_the_job_unlocked(self, jid):
        expect(client.get('q:job:%s:expires' % (jid))).to_equal(None)

    class AndWhenWeClaimTheJob(Vows.Context):
        def topic(self, jid):
            return qluster.job.Job(jid, port=PORT)

        def there_are_no_errors(self, job):
            expect(job).Not.to_be_an_error()

        def the_job_is_now_locked(self, job):
            expect(client.get('q:job:%s:expires' % (job.job_id))).Not.to_equal(None)

        def the_job_expires_in_the_future(self, job):
            expiration = float(client.get('q:job:%s:expires' % (job.job_id)))
            expect(expiration).to_be_greater_than(time.time())
            expect(job.timeToLive()).to_be_greater_than(0)

        def we_can_update_its_state(self, job):
            job.setState('leavening')
            expect(client.zrange('q:jobs:inactive', 0, -1)).to_equal([])
            expect(client.zrange('q:jobs:baking:inactive', 0, -1)).to_equal([])
            expect(client.zrange('q:jobs:leavening', 0, -1)).to_equal([job.job_id])
            expect(client.zrange('q:jobs:baking:leavening', 0, -1)).to_equal([job.job_id])

        class IfAnotherWorkerTriesToClaimIt(Vows.Context):
            def topic(self, job):
                return qluster.job.Job(jid, port=PORT)

            def it_raises_an_exception(self, topic):
                expect(topic).to_be_an_error()

        class IfWeLetItTimeOut(Vows.Context):
            def topic(self, job):
                job.set('timeout', .1)
                time.sleep(.2)
                return job

            def it_is_expired(self, job):
                expect(job.expiration).to_be_less_than(time.time())
                expect(job.timeToLive()).to_equal(0)
                expect(float(client.get('q:job:%s:expires' % (job.job_id)))).to_be_less_than(time.time())

            class WeCanReclaimIt(Vows.Context):
                def topic(self, job):
                    job.claim()
                    return job

                def assuming_we_get_there_first(self, job):
                    expect(job).Not.to_be_an_error()
                    expect(job.expiration).to_be_greater_than(time.time())
                    expect(float(client.get('q:job:%s:expires' % (job.job_id)))).to_be_greater_than(time.time())

                class AndWhenWeRemoveIt(Vows.Context):
                    def topic(self, job):
                        job.remove()
                        return job

                    def it_is_gone_from_the_zsets(self, job):


            
            


        

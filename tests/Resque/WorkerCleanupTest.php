<?php

namespace Resque\Tests;

use PHPUnit\Framework\TestCase;
use Resque\Redis;
use Resque\Worker;

/**
 * Needs a real Redis: RESQUE_TEST_REDIS_HOST (and optionally RESQUE_TEST_REDIS_PORT), the test is
 * skipped without it.
 */
class WorkerCleanupTest extends TestCase
{
    private $siblingProcess = null;

    public function setUp()
    {
        $host = getenv('RESQUE_TEST_REDIS_HOST');

        if (!$host) {
            $this->markTestSkipped('RESQUE_TEST_REDIS_HOST is not set');
        }

        $port = getenv('RESQUE_TEST_REDIS_PORT') ?: 6379;

        $this->setInstance(new RaceRedis(array(
            'scheme'    => 'tcp',
            'host'      => $host,
            'port'      => (int)$port,
            'namespace' => 'resque_cleanup_test_'.getmypid(),
        )));
        $this->deleteTestKeys();
    }

    public function tearDown()
    {
        if ($this->siblingProcess) {
            proc_terminate($this->siblingProcess);
            proc_close($this->siblingProcess);
        }

        $this->deleteTestKeys();

        $this->setInstance(null);
    }

    /**
     * A sibling that registers while cleanup() is running must not get its hash expired, whichever of
     * the two reads cleanup() does first.
     */
    public function testCleanupDoesNotExpireHashOfWorkerRegisteringMidCleanup()
    {
        $redis = Redis::instance();

        $cleaner = new Worker('queue', false);
        $cleaner->setId($cleaner->getHost().':'.getmypid());
        $cleaner->register();

        // cleanup() prunes a local worker whose pid is not alive, so the sibling needs a live pid.
        $this->siblingProcess = proc_open('sleep 30', array(), $pipes);
        $status               = proc_get_status($this->siblingProcess);

        $sibling = new Worker('queue', false);
        $sibling->setId($sibling->getHost().':'.$status['pid']);

        // A hash whose id is not in the set is a real orphan and has to be expired.
        $orphanId = $cleaner->getHost().':'.(getmypid() + 2);
        $redis->hmset('worker:'.$orphanId, array('hostname' => (string)$cleaner->getHost(), 'processed' => 1));

        $redis->registerOnFirstCleanupRead($sibling);
        $cleaner->cleanup();

        $this->assertEquals(1, $redis->sismember('workers', (string)$sibling), 'sibling is registered');
        $this->assertEquals(-1, $redis->ttl('worker:'.$sibling), 'live sibling keeps a hash without TTL');
        $this->assertGreaterThan(0, $redis->ttl('worker:'.$orphanId), 'orphan hash is expired');
    }

    private function deleteTestKeys()
    {
        $redis = Redis::instance();

        // Keys live under this test's own namespace, so the rest of the database is untouched.
        foreach ((array)$redis->keys('*') as $key) {
            $redis->del($redis->removeNamespace($key));
        }
    }

    private function setInstance($instance)
    {
        $property = new \ReflectionProperty('Resque\Redis', 'instance');
        $property->setAccessible(true);
        $property->setValue(null, $instance);
    }
}

/**
 * Registers a worker right after the first of the two reads that cleanup() starts with, which is the
 * moment a sibling started by the same supervisor lands between them.
 */
class RaceRedis extends Redis
{
    private $sibling = null;

    public function registerOnFirstCleanupRead(Worker $sibling)
    {
        $this->sibling = $sibling;
    }

    public function __call($method, $parameters)
    {
        $result = parent::__call($method, $parameters);

        $isCleanupRead = ($method === 'smembers' && $parameters[0] === 'workers')
            || ($method === 'keys' && strpos($parameters[0], 'worker:') === 0);

        if ($this->sibling !== null && $isCleanupRead) {
            $sibling       = $this->sibling;
            $this->sibling = null;
            $sibling->register();
        }

        return $result;
    }
}

/**
 *    Copyright 2015-2017 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.mybatis.caches.redis;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.ibatis.cache.Cache;

import redis.clients.jedis.*;

/**
 * Cache adapter for Redis.
 *
 * @author Eduardo Macarron
 */
public final class RedisCache implements Cache {

  private final ReadWriteLock readWriteLock = new DummyReadWriteLock();

  private String id;

  private static JedisPool pool;

  private static JedisSentinelPool jedisSentinelPool;

  private static JedisCluster jedisCluster;

  private static RedisConfig redisConfig;

  private static boolean isSetPassword = true;

  private static boolean isCluster = false;

  public RedisCache(final String id) {
    if (id == null) {
      throw new IllegalArgumentException("Cache instances require an ID");
    }
    this.id = id;
    initPool();
  }

  private void initPool() {
    redisConfig = RedisConfigurationBuilder.getInstance().parseConfiguration();
    if(null==redisConfig.getPassword() || redisConfig.getPassword().length() <= 0 ){
      isSetPassword = false;
    }

    if(null==redisConfig.getConnectionMode()){
      redisConfig.setConnectionMode("simple");
    }

    if("cluster".equals(redisConfig.getConnectionMode())){
      isCluster = true;
    }

    if("sentinel".equals(redisConfig.getConnectionMode())){
      Set<String> sentinelNodes = new HashSet<String>();
      if(null==redisConfig.getSentinelNodes() || redisConfig.getSentinelNodes().indexOf(":")==-1){
        throw new IllegalArgumentException("sentinelNodes set error! format:[HOST:PORT,HOST:PORT,...]");
      }
      for(String hostPort:redisConfig.getSentinelNodes().split(",")){
        sentinelNodes.add(hostPort);
      }
      if(isSetPassword){
        jedisSentinelPool = new JedisSentinelPool(redisConfig.getSentinelMasterName(),sentinelNodes,redisConfig,
                redisConfig.getConnectionTimeout(),redisConfig.getSoTimeout(),redisConfig.getPassword(),
                redisConfig.getDatabase(),redisConfig.getClientName());
      }else{
        jedisSentinelPool = new JedisSentinelPool(redisConfig.getSentinelMasterName(),sentinelNodes,redisConfig,
                redisConfig.getSoTimeout());
      }

    }else if("cluster".equals(redisConfig.getConnectionMode())){
      Set<HostAndPort> hostAndPorts = new HashSet<HostAndPort>();
      if(null==redisConfig.getClusterNodes()|| redisConfig.getClusterNodes().indexOf(":")==-1
              || redisConfig.getClusterNodes().indexOf(",")==-1){
        throw new IllegalArgumentException("clusterNodes set error! format:[HOST:PORT,HOST:PORT,...]");
      }

      for(String hostPort:redisConfig.getClusterNodes().split(",")){
        String [] hp = hostPort.split(":");
        HostAndPort hostAndPort = new HostAndPort(hp[0],Integer.valueOf(hp[1]));
        hostAndPorts.add(hostAndPort);
      }

      if(isSetPassword){
        jedisCluster = new JedisCluster(hostAndPorts,redisConfig.getConnectionTimeout(),redisConfig.getSoTimeout(),
                redisConfig.getMaxAttempts(),redisConfig.getPassword(),redisConfig);
      }else{
        jedisCluster = new JedisCluster(hostAndPorts,redisConfig.getConnectionTimeout(),redisConfig.getSoTimeout(),
                redisConfig.getMaxAttempts(),redisConfig);
      }

    }else{
      pool = new JedisPool(redisConfig, redisConfig.getHost(), redisConfig.getPort(),
              redisConfig.getConnectionTimeout(), redisConfig.getSoTimeout(), redisConfig.getPassword(),
              redisConfig.getDatabase(), redisConfig.getClientName(), redisConfig.isSsl(),
              redisConfig.getSslSocketFactory(), redisConfig.getSslParameters(), redisConfig.getHostnameVerifier());
    }
  }

  // TODO Review this is UNUSED
  private Object execute(RedisCallback callback) {
    Jedis jedis = getResource();
    try {
      return callback.doWithRedis(jedis);
    } finally {
      jedis.close();
    }
  }

  private Jedis getResource() {
    if("sentinel".equals(redisConfig.getConnectionMode())){
      return jedisSentinelPool.getResource();
    }else{
      return pool.getResource();
    }
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public int getSize() {
    if(isCluster){
      Map<byte[], byte[]> result = jedisCluster.hgetAll(id.getBytes());
      return result.size();
    }
    return (Integer) execute(new RedisCallback() {
      @Override
      public Object doWithRedis(Jedis jedis) {
        Map<byte[], byte[]> result = jedis.hgetAll(id.getBytes());
        return result.size();
      }
    });
  }

  @Override
  public void putObject(final Object key, final Object value) {
    if(isCluster){
      jedisCluster.hset(id.getBytes(), key.toString().getBytes(), SerializeUtil.serialize(value));
      return;
    }
    execute(new RedisCallback() {
      @Override
      public Object doWithRedis(Jedis jedis) {
        jedis.hset(id.getBytes(), key.toString().getBytes(), SerializeUtil.serialize(value));
        return null;
      }
    });
  }

  @Override
  public Object getObject(final Object key) {
    if(isCluster){
      return SerializeUtil.unserialize(jedisCluster.hget(id.getBytes(), key.toString().getBytes()));
    }
    return execute(new RedisCallback() {
      @Override
      public Object doWithRedis(Jedis jedis) {
        return SerializeUtil.unserialize(jedis.hget(id.getBytes(), key.toString().getBytes()));
      }
    });
  }

  @Override
  public Object removeObject(final Object key) {
    if(isCluster){
      return jedisCluster.hdel(id, key.toString());
    }
    return execute(new RedisCallback() {
      @Override
      public Object doWithRedis(Jedis jedis) {
        return jedis.hdel(id, key.toString());
      }
    });
  }

  @Override
  public void clear() {
    if(isCluster){
      jedisCluster.del(id);
      return;
    }
    execute(new RedisCallback() {
      @Override
      public Object doWithRedis(Jedis jedis) {
        jedis.del(id);
        return null;
      }
    });

  }

  @Override
  public ReadWriteLock getReadWriteLock() {
    return readWriteLock;
  }

  @Override
  public String toString() {
    return "Redis {" + id + "}";
  }

}

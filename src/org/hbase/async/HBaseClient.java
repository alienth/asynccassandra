package org.hbase.async;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class HBaseClient {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);
  
  public static final byte[] EMPTY_ARRAY = new byte[0];
  
  /** A byte array containing a single zero byte.  */
  static final byte[] ZERO_ARRAY = new byte[] { 0 };
  
  final JedisPool jedisPool;
  final Config config;

  //------------------------ //
  // Client usage statistics. //
  // ------------------------ //
  
  /** Number of calls to {@link #flush}.  */
  private final AtomicLong num_flushes = new AtomicLong();
  
  /** Number of calls to {@link #get}.  */
  private final AtomicLong num_gets = new AtomicLong();
  
  /** Number of calls to {@link #openScanner}.  */
  private final AtomicLong num_scanners_opened = new AtomicLong();
  
  /** Number of calls to {@link #scanNextRows}.  */
  private final AtomicLong num_scans = new AtomicLong();
  
  /** Number calls to {@link #put}.  */
  private final AtomicLong num_puts = new AtomicLong();
   
  /** Number calls to {@link #lockRow}.  */
  private final AtomicLong num_row_locks = new AtomicLong();
   
  /** Number calls to {@link #delete}.  */
  private final AtomicLong num_deletes = new AtomicLong();
  
  /** Number of {@link AtomicIncrementRequest} sent.  */
  private final AtomicLong num_atomic_increments = new AtomicLong();

  public HBaseClient(final Config config) {
    this.config = config;
    if (config.getString("redis.server") == null || 
        config.getString("redis.server").isEmpty()) {
      throw new IllegalArgumentException(
          "Missing required config 'redis.server'");
    }
    jedisPool = new JedisPool(new JedisPoolConfig(), config.getString("redis.server"));
  }
  
  static short METRICS_WIDTH = 3;
  static short TAG_NAME_WIDTH = 3;
  static short TAG_VALUE_WIDTH = 3;
  static final short TIMESTAMP_BYTES = 4;
  static short SALT_WIDTH = 0;

  public Deferred<Object> lpush(final PutRequest request) {
    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();
      jedis.lpush(request.key(), request.value());
      jedis.ltrim(request.key(), 0, 60 * 60 * 3);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
    return Deferred.fromResult(null);
  }

  public Deferred<Object> hsetnx(final PutRequest request) {
    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();
      jedis.hsetnx(request.key(), request.value(), ZERO_ARRAY);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
    return Deferred.fromResult(null);
  }

  /**
   * UNUSED at this time. Eventually we may store BigTable stats in these
   * objects. It is here for backwards compatability with AsyncHBase.
   * @return An empty list.
   */
  public List<RegionClientStats> regionStats() {
    return Collections.emptyList();
  }
  
  public Scanner newScanner(final Object table) {
    num_scanners_opened.incrementAndGet();
    return new Scanner(this);
  }
  
  public Deferred<Object> shutdown() {
    try {
      jedisPool.destroy();
    } catch (Exception e) {
      LOG.error("failed to close connection to redis", e);
    }
    return Deferred.fromResult(null);
  }
  
  public ClientStats stats() {
    return new ClientStats(0, 0, 0, 0, 
        num_flushes.get(), 
        0, 0, 0, 
        num_gets.get(), 
        num_scanners_opened.get(), 
        num_scans.get(), 
        num_puts.get(), 
        0, 
        num_row_locks.get(), 
        num_deletes.get(), 
        num_atomic_increments.get(), 
        null);
  }
  
  void incrementScans(){
    num_scans.incrementAndGet();
  }
  
}

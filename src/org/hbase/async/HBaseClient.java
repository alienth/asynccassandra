package org.hbase.async;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.microsoft.sqlserver.jdbc.*;
import java.sql.*;
import org.apache.commons.dbcp2.*;

public class HBaseClient {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);
  
  public static final byte[] EMPTY_ARRAY = new byte[0];
  
  /** A byte array containing a single zero byte.  */
  static final byte[] ZERO_ARRAY = new byte[] { 0 };
  
  final JedisPool jedisPool;
  final BasicDataSource connectionPool;
  final Config config;
  final ExecutorService executor = Executors.newFixedThreadPool(25);

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

    connectionPool = new BasicDataSource();
    connectionPool.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    connectionPool.setUrl(String.format("jdbc:sqlserver://%s;user=%s;password=%s", config.getString("sql.server"), config.getString("sql.user"), config.getString("sql.password")));
    connectionPool.setInitialSize(10);

    Connection connection = null;
    try {
      connection = connectionPool.getConnection();
      // Statement stmt = connection.createStatement();
      // stmt.executeUpdate("CREATE TABLE test (test timestamp)");
      // ResultSet rs = stmt.executeQuery("SELECT * from [dbo].[os.cpu]");
      // ResultSet rs = connection.getMetaData().getTables(null, "dbo", "%", null);
      // while (rs.next()) {
      //   LOG.warn(rs.toString());
      // }
    } catch (Exception e) {
      LOG.warn(e + "");
    } finally {
      if (connection != null) {
        try {
        connection.close();
        } catch (SQLException e) {
          // boo
        }
      }
    }

    final TrimThread thread = new TrimThread();
    thread.setDaemon(true);
    thread.start();
  }
  
  private static final MaxSizeHashMap<ByteBuffer, Boolean> indexedKeys = new MaxSizeHashMap<ByteBuffer, Boolean>(1000000);

  private static class MaxSizeHashMap<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = 1L;
    private final int maxSize;

    public MaxSizeHashMap(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > maxSize;
    }
  }


  static short METRICS_WIDTH = 3;
  static short TAG_NAME_WIDTH = 3;
  static short TAG_VALUE_WIDTH = 3;
  static final short TIMESTAMP_BYTES = 4;
  static short SALT_WIDTH = 0;

  private Map<String, List<byte[]>> buffered_lpush = new HashMap<String, List<byte[]>>();
  private final AtomicLong num_buffered_pushes = new AtomicLong();

  private static final Charset CHARSET = Charset.forName("ISO-8859-1");

  public Deferred<Object> insert(final String metric, final long timestamp, Map<String, String> tagm, final float value) {
    // String key = new String(request.key(), CHARSET);
    // synchronized (buffered_lpush) {
    //   List<byte[]> lpushes = buffered_lpush.get(key);
    //   if (lpushes == null) {
    //     lpushes = new ArrayList<byte[]>();
    //     buffered_lpush.put(key, lpushes);
    //   }
    //   lpushes.add(request.value());
    //   if (num_buffered_pushes.incrementAndGet() >= config.getInt("hbase.rpcs.batch.size")) {
    //     num_buffered_pushes.set(0);
    //     Map<String, List<byte[]>> lpush_batch = buffered_lpush;
    //     buffered_lpush = new HashMap<String, List<byte[]>>();
    //     return lpushInternal(lpush_batch);
    //   }
    // }

    try (Connection connection = connectionPool.getConnection()) {
      final StringBuilder columns = new StringBuilder(100);
      final StringBuilder values = new StringBuilder(20);
      final String[] keys = new String[tagm.size()];
      tagm.keySet().toArray(keys);
      for (String key : keys) {
        columns.append(key);
        columns.append(", ");
        values.append("?,");
      }

      // TODO: prevent injection
      String insert = String.format("INSERT INTO [dbo].[%s] (%s date, time, value) VALUES (%s ?, ?, ?)", metric, columns, values);
      PreparedStatement stmt = connection.prepareStatement(insert);
      // stmt.setString(1, metric);

      final int tagCount = tagm.size();
      for (int i = 0; i < tagCount; i++) {
        stmt.setString(i+1, tagm.get(keys[i]));
      }
      stmt.setDate(tagCount+1, new Date(timestamp));
      stmt.setTime(tagCount+2, new Time(timestamp));
      stmt.setFloat(tagCount+3, value);
      stmt.executeUpdate();
      // stmt.setDate(request.t
      // Statement stmt = connection.createStatement();

    } catch (SQLException e) {
      return Deferred.fromError(e);
    }
    return Deferred.fromResult(null);
  }

  private Set<String> metrics = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

  private Deferred<Object> lpushInternal(Map<String, List<byte[]>> lpushes) {
    try (Jedis jedis = jedisPool.getResource()) {
      for (Entry<String, List<byte[]>> row : lpushes.entrySet()) {
          final byte[][] values = new byte[row.getValue().size()][];
          row.getValue().toArray(values);
          jedis.lpush(row.getKey().getBytes(CHARSET), values);
      }
    } catch (Exception e) {
        LOG.warn(e.toString());
        return Deferred.fromError(e);
    }

    return Deferred.fromResult(null);
  }

  public Deferred<Object> hsetnx(final PutRequest request) {
    synchronized (indexedKeys) {
      byte[] check = new byte[request.key().length + request.value().length];
      System.arraycopy(request.key(), 0, check, 0, request.key().length);
      System.arraycopy(request.value(), 0, check, request.key().length, request.value().length);
      if (indexedKeys.put(ByteBuffer.wrap(check), true) != null) {
        // We already indexed this
        return Deferred.fromResult(null);
      }
    }

    metrics.add(new String(request.key(), CHARSET));

    try (Jedis jedis = jedisPool.getResource()) {
      jedis.hsetnx(request.key(), request.value(), ZERO_ARRAY);
    } catch (Exception e) {
      LOG.warn(e.toString());
      return Deferred.fromError(e);
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
    return new Scanner(this, executor);
  }
  
  public Deferred<Object> shutdown() {
    try {
      jedisPool.destroy();
      executor.shutdown();
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
  

  final class TrimThread extends Thread {
    public TrimThread() {
      super("TrimThread");
    }

    @Override
    public void run() {
      while (true) {
        for (String metricStr : metrics) {
          final byte[] metric = metricStr.getBytes(CHARSET);
          try (Jedis jedis = jedisPool.getResource()) {
            Set<byte[]> tags = jedis.hkeys(metric);
            for (byte[] tag : tags) {
              byte[] key = new byte[metric.length + 1 + tag.length];
              System.arraycopy(metric, 0, key, 0, metric.length);
              System.arraycopy(tag, 0, key, metric.length + 1, tag.length);
              jedis.ltrim(key, 0, 1000);
            }
          } catch (Exception e) {
            LOG.error("Error while performing periodic ltrim: " + e);
          }
        }
        try {
          Thread.sleep(600000);
        } catch (InterruptedException e) {
            LOG.error("Trim thread interrupted", e);
            return;
        }
      }
    }

  }

}

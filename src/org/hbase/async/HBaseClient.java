package org.hbase.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import com.microsoft.sqlserver.jdbc.*;

import java.sql.*;

import org.apache.commons.dbcp2.*;

public class HBaseClient {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);
  
  public static final byte[] EMPTY_ARRAY = new byte[0];
  
  /** A byte array containing a single zero byte.  */
  static final byte[] ZERO_ARRAY = new byte[] { 0 };
  
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

  }
  
  private class Datapoint {
    public final String metric;
    public final Map<String, String> tagm;
    public final long timestamp;
    public final double value;

    public Datapoint(final String metric, final Map<String, String> tagm, final long timestamp, final double value) {
      this.metric = metric;
      this.tagm = tagm;
      this.timestamp = timestamp;
      this.value = value;
    }
  }

  // When a new datapoint comes in, we grab a batch by the metric it came in on.
  // We check to ensure the batch is aware of all of the tag keys.

  private class DatapointBatch implements ISQLServerBulkRecord {
    private int valueOrd = 1;
    private int timestampOrd = 2;

    // private final HashMap<String, String> tagm = new HashMap<String, String>();
    // private final double value = 0;
    // private final Timestamp timestamp;

    private final List<Datapoint> dps = new ArrayList<Datapoint>();

    private final List<String> tags = new ArrayList<String>();


    private int cursor = 0;

    public void add(Datapoint d) {
      dps.add(d);
      for (String tag : d.tagm.keySet()) {
        if (!tags.contains(tag)) {
          tags.add(tag);
        }
      }
    }

    public boolean isAutoIncrement(int column) {
      return false;
    }

    public int getScale(int column) {
      if (column == valueOrd) {
        return 1;
      }
      return 0;
    }

    public String getColumnName(int column) {
      if (column == valueOrd) {
        return "value";
      } else if (column == timestampOrd) {
        return "timestamp";
      }
      return tags.get(column - 3);
    }

    public boolean next() throws SQLServerException {
      cursor++;
      if (cursor > dps.size()) {
        return false;
      }
      return true;
    }

    public Object[] getRowData() throws SQLServerException {
      Datapoint dp = dps.get(cursor - 1);
      Object[] results = new Object[2 + tags.size()];
      results[0] = new Double(dp.value);
      results[1] = new Timestamp(dp.timestamp);
      int i = 2;
      for (String tagk : tags) {
        results[i] = dp.tagm.get(tagk);
        i++;
      }
      return results;
    }

    public Set<Integer> getColumnOrdinals() {
      HashSet<Integer> result = new HashSet<Integer>();

      result.add(1); //value
      result.add(2); //timestamp
      
      int i = 2;
      for (Object ignored : tags) {
        i++;
        result.add(i);
      }
      return result;
    }

    public int getColumnType(int column) {
      if (column == valueOrd) {
        return java.sql.Types.DOUBLE;
      } else if (column == timestampOrd) {
        return java.sql.Types.TIMESTAMP;
      } else {
        return java.sql.Types.VARCHAR;
      }
    }
    public int getPrecision(int column) {
      if (column == valueOrd) {
        return 53;
      }
      return 100;
    }

  }

  static short METRICS_WIDTH = 3;
  static short TAG_NAME_WIDTH = 3;
  static short TAG_VALUE_WIDTH = 3;
  static final short TIMESTAMP_BYTES = 4;
  static short SALT_WIDTH = 0;

  private HashMap<String, Set<String>> tables = new HashMap<String, Set<String>>();

  private Map<String, DatapointBatch> buffered_datapoint = new HashMap<String, DatapointBatch>();
  private final AtomicLong num_buffered_pushes = new AtomicLong();

  public Deferred<Object> insert(final String metric, final Map<String, String> tagm, final long timestamp, final double value) {
    final Set<String> tagSet = tagm.keySet();

    Set<String> tableTags;
    synchronized(tables) {
      tableTags = tables.get(metric);
    }
    if (tableTags == null || ! tableTags.containsAll(tagSet)) {
      try (Connection connection = connectionPool.getConnection()) {
        LOG.warn("Syncing schema for metric " + metric);
        syncSchema(connection, metric, tagSet);
      } catch (SQLException e) {
        return Deferred.fromError(e);
      }
    }

    synchronized (buffered_datapoint) {
      DatapointBatch dps = buffered_datapoint.get(metric);
      if (dps == null) {
        dps = new DatapointBatch();
        buffered_datapoint.put(metric, dps);
      }
      Datapoint dp = new Datapoint(metric, tagm, timestamp, value);
      dps.add(dp);
      if (num_buffered_pushes.incrementAndGet() >= config.getInt("hbase.rpcs.batch.size")) {
        num_buffered_pushes.set(0);
        Map<String, DatapointBatch> batches = buffered_datapoint;
        buffered_datapoint = new HashMap<String, DatapointBatch>();
        return insertInternal(batches);
      }
    }
    return Deferred.fromResult(null);
  }

  public Deferred<Object> insertInternal(Map<String, DatapointBatch> batches) {
    // try (Connection connection = connectionPool.getConnection()) {
    try (Connection connection = DriverManager.getConnection(connectionPool.getUrl())) {

      for (Entry<String, DatapointBatch> entry : batches.entrySet()) {
        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
        bulkCopy.addColumnMapping("value", "value");
        bulkCopy.addColumnMapping("timestamp", "timestamp");
        for (String tag : entry.getValue().tags) {
          bulkCopy.addColumnMapping(tag, "tag." + tag);
        }
        bulkCopy.setDestinationTableName("[" + entry.getKey() + "]");
        bulkCopy.writeToServer(entry.getValue());
        bulkCopy.close();
      }
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
    return Deferred.fromResult(null);
  }

  public void syncSchema(Connection connection, String metric, Set<String> tags) throws SQLException {
    Statement stmt;
    DatabaseMetaData md = connection.getMetaData();
    ResultSet rs = md.getTables(null, "dbo", metric, new String[] {"TABLE"});
    if (!rs.next()) {
      final StringBuilder columnDefs = new StringBuilder(100);
      for (String key : tags) {
        columnDefs.append("[tag.");
        columnDefs.append(key);
        columnDefs.append("] nvarchar(100) NULL,");
      }
      columnDefs.append("timestamp datetime NOT NULL, value float NOT NULL");

      String create = String.format("CREATE TABLE [dbo].[%s] (%s);", metric, columnDefs);
      String index = String.format("CREATE Clustered Columnstore Index [CCI_%s] ON [dbo].[%s];", metric, metric);

      LOG.warn("Creating table " + metric);
      stmt = connection.createStatement();
      stmt.executeUpdate(create);
      stmt = connection.createStatement();
      stmt.executeUpdate(index);

      Set<String> foo = new HashSet<String>(tags);
      synchronized (tables) {
        tables.put(metric, foo);
      }
    } else {
      rs = md.getColumns(null, null, metric, "tag.%");

      final Set<String> columnSet = new HashSet<String>();
      while (rs.next()) {
        ResultSetMetaData rsMeta = rs.getMetaData();
        for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
          String name = rs.getString(i);
          if (name != null && name.startsWith("tag.")) {
            columnSet.add(rs.getString(i).replaceFirst("^tag\\.", ""));
          }
        }
      }

      Set<String> foo = new HashSet<String>(tags);
      foo.removeAll(columnSet);

      if (foo.size() > 0) {
        final StringBuilder alter = new StringBuilder(200);
        alter.append(String.format("ALTER TABLE [dbo].[%s] ADD ", metric));
        for (String column : foo) {
          alter.append(String.format(" [tag.%s] nvarchar(100) NULL,", column));
        }
        alter.setLength(alter.length() - 1);

        LOG.warn("Altering table " + metric);
        stmt = connection.createStatement();
        LOG.warn(alter.toString());
        stmt.executeUpdate(alter.toString());
      }
      Set<String> foo2 = new HashSet<String>(tags);
      synchronized (tables) {
        tables.put(metric, foo2);
      }
    }



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
  

}

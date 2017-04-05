package org.hbase.async;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.hbase.async.Bytes.ByteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnMap;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.AbstractThriftMutationBatchImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class HBaseClient {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);
  
  public static final byte[] EMPTY_ARRAY = new byte[0];
  
  /** A byte array containing a single zero byte.  */
  static final byte[] ZERO_ARRAY = new byte[] { 0 };
  
  public static final ColumnFamily<byte[], byte[]> TSDB_T = new ColumnFamily<byte[], byte[]>(
      "t",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer

  public static final ColumnFamily<byte[], byte[]> TSDB_T_INDEX = new ColumnFamily<byte[], byte[]>(
      "tindex",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer

  public static final ColumnFamily<byte[], byte[]> TSDB_UID_NAME = new ColumnFamily<byte[], byte[]>(
      "name",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer
  
  public static final ColumnFamily<byte[], byte[]> TSDB_UID_ID = new ColumnFamily<byte[], byte[]>(
      "id",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer
  
  public static final ColumnFamily<byte[], String> TSDB_UID_NAME_CAS = 
      new ColumnFamily<byte[], String>(
      "name",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      StringSerializer.get());  // Column Serializer
  
  public static final ColumnFamily<byte[], String> TSDB_UID_ID_CAS = 
      new ColumnFamily<byte[], String>(
      "id",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      StringSerializer.get());  // Column Serializer
  
  final Config config;
  final ExecutorService executor = Executors.newFixedThreadPool(25);
  final ListeningExecutorService service = 
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(25));
  final ByteMap<AstyanaxContext<Keyspace>> contexts =
      new ByteMap<AstyanaxContext<Keyspace>>();
  final ByteMap<Keyspace> keyspaces = new ByteMap<Keyspace>();
  final ByteMap<MutationBatch> mutations = new ByteMap<MutationBatch>();
  final ByteMap<ColumnFamily<byte[], byte[]>> column_family_schemas = 
      new ByteMap<ColumnFamily<byte[], byte[]>>();
  
  final AstyanaxConfigurationImpl ast_config;
  final ConnectionPoolConfigurationImpl pool;
  final CountingConnectionPoolMonitor monitor;

  final ByteMap<AtomicLong> buffer_tracker = new ByteMap<AtomicLong>();
  
  final byte[] tsdb_table;
  final byte[] tsdb_uid_table;
  final int lock_timeout = 5000;
  
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
    if (config.getString("asynccassandra.seeds") == null || 
        config.getString("asynccassandra.seeds").isEmpty()) {
      throw new IllegalArgumentException(
          "Missing required config 'asynccassandra.seeds'");
    }
    
    final int num_workers = config.hasProperty("asynccassandra.workers.size") ?
        config.getInt("asynccassandra.workers.size") :
          Runtime.getRuntime().availableProcessors() * 2;

    ast_config = new AstyanaxConfigurationImpl()      
      .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
      .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_ANY)
      .setAsyncExecutor(
          Executors.newFixedThreadPool(num_workers, new ThreadFactoryBuilder().setDaemon(true)
              .setNameFormat("AstyanaxAsync-%d")
              .build()));
    pool = new ConnectionPoolConfigurationImpl("MyConnectionPool")
      .setPort(config.getInt("asynccassandra.port"))
      .setMaxConnsPerHost(config.getInt("asynccassandra.max_conns_per_host"))
      .setSeeds(config.getString("asynccassandra.seeds"));
    if (config.hasProperty("asynccassandra.datacenter")) {
      pool.setLocalDatacenter(config.getString("asynccassandra.datacenter"));
    }
    monitor = new CountingConnectionPoolMonitor();
    METRICS_WIDTH = config.hasProperty("tsd.storage.uid.width.metric") ?
      config.getShort("tsd.storage.uid.width.metric") : 3;
    TAG_NAME_WIDTH = config.hasProperty("tsd.storage.uid.width.tagk") ?
      config.getShort("tsd.storage.uid.width.metric") : 3;
    TAG_VALUE_WIDTH = config.hasProperty("tsd.storage.uid.width.tagv") ?
      config.getShort("tsd.storage.uid.width.metric") : 3;

    if (config.hasProperty("tsd.storage.salt.width")) {
      SALT_WIDTH = config.getShort("tsd.storage.salt.width");
    }
    
    tsdb_table = config.getString("tsd.storage.hbase.data_table").getBytes();
    tsdb_uid_table = config.getString("tsd.storage.hbase.uid_table").getBytes();
    
    column_family_schemas.put("t".getBytes(), TSDB_T);
    column_family_schemas.put("name".getBytes(), TSDB_UID_NAME);
    column_family_schemas.put("id".getBytes(), TSDB_UID_ID);
  }
  
  ByteMap<ColumnFamily<byte[], byte[]>> getColumnFamilySchemas() {
    return column_family_schemas;
  }
  
  public Deferred<ArrayList<KeyValue>> get(final GetRequest request) {
    num_gets.incrementAndGet();
    final Keyspace keyspace = getContext(request.table);
    if (request.family() == null) {
      throw new UnsupportedOperationException(
          "Can't scan cassandra without a column family: " + request);
    }
    
    final Deferred<ArrayList<KeyValue>> deferred = 
        new Deferred<ArrayList<KeyValue>>();
    
    class FutureCB implements FutureCallback<OperationResult<ColumnList<byte[]>>> {

      @Override
      public void onFailure(Throwable e) {
        deferred.callback(e);
      }

      @Override
      public void onSuccess(OperationResult<ColumnList<byte[]>> result) {
        try {
          // TODO - can track stats here
          final ColumnList<byte[]> columns = result.getResult();
          final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(columns.size());
          final Iterator<Column<byte[]>> it = columns.iterator();
          while (it.hasNext()) {
            final Column<byte[]> column = it.next();
            final KeyValue kv = new KeyValue(request.key, request.family(), 
                column.getName(), column.getTimestamp() / 1000, // micro to ms 
                column.getByteArrayValue());
            kvs.add(kv);
          }
          deferred.callback(kvs);
        } catch (RuntimeException e) {
          deferred.callback(e);
        }
      }
    }
    
    // Sucks, have to have a family I guess
    try {
      final ListenableFuture<OperationResult<ColumnList<byte[]>>> future; 
      final ColumnFamilyQuery<byte[], byte[]> cfquery = keyspace.prepareQuery(
          column_family_schemas.get(request.family()));
      if (Bytes.memcmp(tsdb_uid_table, request.table()) != 0) {
        // Force quorum lookups for IDs
        cfquery.setConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM);
      }
      final RowQuery<byte[], byte[]> query = cfquery.getKey(request.key);
      if (request.qualifiers() == null || request.qualifiers().length < 1) {
        future = query.executeAsync();
      } else {
        future = query.withColumnSlice(
            Arrays.asList(request.qualifiers())).executeAsync();
      }
      Futures.addCallback(future, new FutureCB(), service);
    } catch (ConnectionException e) {
      deferred.callback(e);
    }
    
    return deferred;
  }

  static short METRICS_WIDTH = 3;
  static short TAG_NAME_WIDTH = 3;
  static short TAG_VALUE_WIDTH = 3;
  static final short TIMESTAMP_BYTES = 4;
  static short SALT_WIDTH = 0;

  private class MaxSizeHashMap<K, V> extends LinkedHashMap<K, V> {
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

  private final MaxSizeHashMap<ByteBuffer, Boolean> indexedKeys = new MaxSizeHashMap<ByteBuffer, Boolean>(30000);

  private void indexMutation(byte[] orig_key, MutationBatch mutation) {
    // Take the metric of the orig key and put it in the new key
    // Take the timestamp of the orig key, normalize it to a month, and put it in the new key
    // Take the timestamp of the orig key and put it in the column name.
    // Take only the tags from the orig key, and put them the column name.

    synchronized (indexedKeys) {
      if (indexedKeys.put(ByteBuffer.wrap(orig_key), true) != null) {
        // We already indexed this key
        return;
      }
    }

    // Take the tags out of the orig key and place them in the column
    final byte[] ts = Arrays.copyOfRange(orig_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES + SALT_WIDTH + METRICS_WIDTH);
    final int tsInt = Bytes.getInt(ts);
    int month = tsInt - (tsInt % (86400 * 28));
    byte[] new_key = new byte[SALT_WIDTH + METRICS_WIDTH + TIMESTAMP_BYTES];
    final byte[] new_col = Arrays.copyOfRange(orig_key, METRICS_WIDTH + SALT_WIDTH + TIMESTAMP_BYTES, orig_key.length);
    System.arraycopy(orig_key, 0, new_key, 0, SALT_WIDTH + METRICS_WIDTH);
    System.arraycopy(Bytes.fromInt(month), 0, new_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES);
    // System.arraycopy(ts, 0, new_col, 0, ts.length);
    // System.arraycopy(orig_key, SALT_WIDTH + METRICS_WIDTH + TIMESTAMP_BYTES, new_col, TIMESTAMP_BYTES, new_col.length - TIMESTAMP_BYTES);

    mutation.withRow(TSDB_T_INDEX, new_key).putColumn(new_col, new byte[]{0});
  }

  /** Mask for the millisecond qualifier flag */
  public static final byte MS_BYTE_FLAG = (byte)0xF0;

  /** Flag to set on millisecond qualifier timestamps */
  public static final int MS_FLAG = 0xF0000000;

  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short FLAG_BITS = 4;

  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short MS_FLAG_BITS = 6;

  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  public static final short FLAG_FLOAT = 0x8;

  /** Mask to select the size of a value from the qualifier.  */
  public static final short LENGTH_MASK = 0x7;

  /** Mask to select all the FLAG_BITS.  */
  public static final short FLAGS_MASK = FLAG_FLOAT | LENGTH_MASK;

  public static int getOffsetFromQualifier(final byte[] qualifier, 
      final int offset) {
    // validateQualifier(qualifier, offset);
    if ((qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG) {
      return (int)(Bytes.getUnsignedInt(qualifier, offset) & 0x0FFFFFC0) 
        >>> MS_FLAG_BITS;
    } else {
      final int seconds = (Bytes.getUnsignedShort(qualifier, offset) & 0xFFFF) 
        >>> FLAG_BITS;
      return seconds * 1000;
    }
  }

 public static short getFlagsFromQualifier(final byte[] qualifier, 
      final int offset) {
    // validateQualifier(qualifier, offset);
    if ((qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG) {
      return (short) (qualifier[offset + 3] & FLAGS_MASK); 
    } else {
      return (short) (qualifier[offset + 1] & FLAGS_MASK);
    }
  }



  private void tMutation(PutRequest request, MutationBatch mutation) {
    // Take the timestamp of the orig key, normalize it to the 28-day period, and put it in the new key.
    // Take the metric + tags of the orig key and put it in the new key.
    // Take the offset from the column, add it to the difference between the orig ts and the new base, and put it in the column.
    //
    // If the column is in seconds, we'll use 22 bits to store the offset.
    // If the column is in MS, we'll need 31 bits to store the offset. - DEPRECATING
    // We need 4 bits for the format flag.

    final byte[] ts = Arrays.copyOfRange(request.key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES + SALT_WIDTH + METRICS_WIDTH);
    final int tsInt = Bytes.getInt(ts);
    int month = tsInt - (tsInt % (86400 * 28));

    final int offset = (tsInt - month) + (getOffsetFromQualifier(request.qualifier(), 0) / 1000);
    final int flags = getFlagsFromQualifier(request.qualifier(), 0);
    final byte[] new_col = Bytes.fromInt(offset << 10 | flags);


    byte[] new_key = Arrays.copyOf(request.key, request.key.length);
    System.arraycopy(Bytes.fromInt(month), 0, new_key, SALT_WIDTH + METRICS_WIDTH, TIMESTAMP_BYTES);

    // byte[] new_col = new byte[new_offset.length + flags.length];
    // System.arraycopy(new_offset, 0, new_col, 0, new_offset.length);
    // System.arraycopy(flags, 0, new_col, new_offset.length, flags.length);
    mutation.withRow(column_family_schemas.get(request.family), new_key)
      .putColumn(new_col, request.value());

    // TODO: We only need to check this once a month now.
    synchronized (indexedKeys) {
      if (indexedKeys.put(ByteBuffer.wrap(request.key), true) != null) {
        // We already indexed this key
        return;
      }
    }

    // Take the tags out of the orig key and place them in the column
    byte[] index_key = Arrays.copyOfRange(new_key, 0, SALT_WIDTH + METRICS_WIDTH + TIMESTAMP_BYTES);
    final byte[] index_col = Arrays.copyOfRange(request.key, METRICS_WIDTH + SALT_WIDTH + TIMESTAMP_BYTES, request.key.length);

    mutation.withRow(TSDB_T_INDEX, index_key).putColumn(index_col, new byte[]{0});
  }

  public Deferred<Object> put(final PutRequest request) {
    final Keyspace keyspace = getContext(request.table);
    final MutationBatch mutation = keyspace.prepareMutationBatch();
    final AtomicLong buffer_count = buffer_tracker.get(request.table);
    if (Bytes.memcmp("t".getBytes(), request.family) == 0) {
      tMutation(request, mutation);
    } else {
      mutation.withRow(column_family_schemas.get(request.family), request.key)
        .putColumn(request.qualifier(), request.value());
    }
    synchronized (mutations) {
      final MutationBatch buffered_mutations = mutations.get(request.table);
      buffered_mutations.mergeShallow(mutation);
      final long count = buffer_count.incrementAndGet();
      if (count >= config.getInt("hbase.rpcs.batch.size")) {
        buffer_count.set(0);
        mutations.put(request.table, keyspace.prepareMutationBatch());
        return putInternal(buffered_mutations);
      } else {
        return Deferred.fromResult(null);
      }
    }
  }

  public Deferred<Object> putInternal(final MutationBatch mutation) {
    num_puts.incrementAndGet();
    final Deferred<Object> deferred = new Deferred<Object>();
    try {
      final ListenableFuture<OperationResult<Void>> future = mutation.executeAsync();
      
      class ResponseCB implements Runnable {
        @Override
        public void run() {
          try {
            future.get().getResult();
            deferred.callback(null);
          } catch (InterruptedException e) {
            deferred.callback(e);
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            deferred.callback(e);
          }
        }
      }
      
      class PutCB implements FutureCallback<OperationResult<Void>> {

        @Override
        public void onFailure(Throwable e) {
          deferred.callback(e);
        }
        
        @Override
        public void onSuccess(OperationResult<Void> arg0) {
          deferred.callback(null);
        }
        
      }
      
      //future.addListener(new ResponseCB(), executor);
      Futures.addCallback(future, new PutCB(), service);
    } catch (ConnectionException e) {
      deferred.callback(e);
    }

    return deferred;
  }
  
  public Deferred<Object> append(final AppendRequest request) {
    return Deferred.fromError(
        new UnsupportedOperationException("Not implemented yet"));
  }
  
  public Deferred<Object> delete(final DeleteRequest request) {
    num_deletes.incrementAndGet();
    // TODO how do we batch?
    final Keyspace keyspace = getContext(request.table);
    final Deferred<Object> deferred = new Deferred<Object>();
    final MutationBatch mutation = keyspace.prepareMutationBatch();
    
    // TODO - all quals
    final ColumnListMutation<byte[]> clm = mutation
        .withRow(column_family_schemas.get(request.family), request.key);
    for (final byte[] qualifier : request.qualifiers()) {
      clm.deleteColumn(qualifier);
    }
    try {
      final ListenableFuture<OperationResult<Void>> future = mutation.executeAsync();
      
      class ResponseCB implements Runnable {
        @Override
        public void run() {
          try {
            future.get().getResult();
            deferred.callback(null);
          } catch (InterruptedException e) {
            deferred.callback(e);
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            deferred.callback(e);
          }
        }
      }
      future.addListener(new ResponseCB(), executor);
    } catch (ConnectionException e) {
      deferred.callback(e);
    }

    return deferred;
  }
  
  public Deferred<Boolean> compareAndSet(final PutRequest edit,
      final byte[] expected) {
    
    if (Bytes.memcmp(tsdb_uid_table, edit.table) != 0) {
      return Deferred.fromError(new UnsupportedOperationException(
          "Increments are not supported on other tables yet"));
    }
    
    final Keyspace keyspace = getContext(edit.table);
    final ColumnFamily<byte[], String> lockCf =
        Bytes.memcmp("id".getBytes(), edit.family) == 0 ? 
            TSDB_UID_ID_CAS : TSDB_UID_NAME_CAS;
    final ColumnFamily<byte[], byte[]> cf =
        Bytes.memcmp("id".getBytes(), edit.family) == 0 ?
            TSDB_UID_ID : TSDB_UID_NAME;
    final byte[] lockKey =
        Bytes.memcmp("id".getBytes(), edit.family) == 0 ?
            edit.qualifier() : edit.value();
    ColumnPrefixDistributedRowLock<byte[]> lock = 
        new ColumnPrefixDistributedRowLock<byte[]>(keyspace, lockCf,
            lockKey)
            .withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
            .withConsistencyLevel(ConsistencyLevel.CL_EACH_QUORUM)
            .withTtl(300)
            .expireLockAfter(lock_timeout, TimeUnit.MILLISECONDS);
    try {
      num_row_locks.incrementAndGet();
      lock.acquire();
      byte[] value = null;
      try {
        value = keyspace.prepareQuery(cf).getKey(edit.key()).getColumn(edit.qualifier()).execute().getResult().getByteArrayValue();
      } catch (NotFoundException e) {
        // The common case - there is no value here.
      }
      final MutationBatch mutation = keyspace.prepareMutationBatch();
      mutation.setConsistencyLevel(ConsistencyLevel.CL_EACH_QUORUM);
      mutation.withRow(cf, edit.key)
        .putColumn(edit.qualifier(), edit.value(), null);
      
      if (value == null && (expected == null || expected.length < 1)) {
        // Have to separate the mutation and the release due to differing serialization types.
        mutation.execute();
        lock.release();
        return Deferred.fromResult(true);
      } else if (expected != null && value != null &&
          Bytes.memcmpMaybeNull(value, expected) == 0) {
        mutation.execute();
        lock.release();
        return Deferred.fromResult(true);
      }
      
      try {
        lock.release();
      } catch (Exception e) {
        LOG.error("Error releasing lock post exception for request: " + edit, e);
      }
      return Deferred.fromResult(false);
    } catch (BusyLockException e) {
        // Wrap the busy lock exception as an HBaseException so that opentsdb tries again.
        return Deferred.fromError(new NonRecoverableException("", e));
    } catch (Exception e) {
      try {
        lock.release();
      } catch (Exception e1) {
        LOG.error("Error releasing lock post exception for request: " + edit, e1);
      }
      return Deferred.fromError(e);
    }
  }
  
  // TODO - async me!
  public Deferred<Long> atomicIncrement(final AtomicIncrementRequest request) {
    num_atomic_increments.incrementAndGet();
    if (Bytes.memcmp(tsdb_uid_table, request.table) != 0) {
      return Deferred.fromError(new UnsupportedOperationException(
          "Increments are not supported on other tables yet"));
    }
    
    final Keyspace keyspace = getContext(request.table);
    ColumnPrefixDistributedRowLock<byte[]> lock = 
        new ColumnPrefixDistributedRowLock<byte[]>(keyspace, 
            TSDB_UID_ID_CAS, request.key)
            .withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
            .withTtl(300) // TODO: Config this?
            .expireLockAfter(lock_timeout, TimeUnit.MILLISECONDS);
    try {
      num_row_locks.incrementAndGet();
      final ColumnMap<String> columns = lock.acquireLockAndReadRow();
              
      // Modify a value and add it to a batch mutation
      final String qualifier = new String(request.qualifier());
      long value = 1;
      if (columns.get(qualifier) != null) {
        value = columns.get(qualifier).getLongValue() + 1;
      }
      final MutationBatch mutation = keyspace.prepareMutationBatch();
      mutation.setConsistencyLevel(ConsistencyLevel.CL_EACH_QUORUM);
      mutation.withRow(TSDB_UID_ID_CAS, request.key)
        .putColumn(qualifier, value, null);
      lock.releaseWithMutation(mutation);
      return Deferred.fromResult(value);
    } catch (BusyLockException e) {
      // Wrap the busy lock exception as an HBaseException so that opentsdb tries again.
      return Deferred.fromError(new NonRecoverableException("", e));
    } catch (Exception e) {
      try {
        lock.release();
      } catch (Exception e1) {
        LOG.error("Error releasing lock post exception for request: " + request, e1);
      }
      
      return Deferred.fromError(e);
    }
  }
  
  // TODO - buffer!
  public Deferred<Long> bufferAtomicIncrement(final AtomicIncrementRequest request) {
    return atomicIncrement(request);
  }
  
  public Deferred<Object> ensureTableExists(final byte[] table) {
    return ensureTableFamilyExists(table, EMPTY_ARRAY);
  }
  
  public Deferred<Object> ensureTableFamilyExists(final byte[] table,
      final byte[] family) {
    // Just "fault in" the first region of the table.  Not the most optimal or
    // useful thing to do but gets the job done for now.  TODO(tsuna): Improve.
    final GetRequest dummy;
    if (family == EMPTY_ARRAY) {
      // figure it out from the table name
      if (Bytes.memcmp(tsdb_table, table) == 0) {
        dummy = GetRequest.exists(table, probeKey(ZERO_ARRAY), 
            TSDB_T.getName().getBytes());
      } else if (Bytes.memcmp(tsdb_uid_table, table) == 0) {
        dummy = GetRequest.exists(table, probeKey(ZERO_ARRAY), 
            TSDB_UID_ID.getName().getBytes());
      } else {
       throw new IllegalArgumentException("Unrecognized table " + Bytes.pretty(table)); 
      }
    } else {
      dummy = GetRequest.exists(table, probeKey(ZERO_ARRAY), family);
    }
    
    class CB implements Callback<Deferred<Object>, ArrayList<KeyValue>> {

      @Override
      public Deferred<Object> call(ArrayList<KeyValue> arg0) throws Exception {
        return Deferred.fromResult(null);
      }
      
    }
    return get(dummy).addCallbackDeferring(new CB());
  }
  
  public Deferred<Object> ensureTableExists(final String table) {
    return ensureTableFamilyExists(table.getBytes(), EMPTY_ARRAY);
  }
  
  /**
   * UNUSED at this time. Eventually we may store BigTable stats in these
   * objects. It is here for backwards compatability with AsyncHBase.
   * @return An empty list.
   */
  public List<RegionClientStats> regionStats() {
    return Collections.emptyList();
  }
  
  public Scanner newScanner(final byte[] table) {
    num_scanners_opened.incrementAndGet();
    return new Scanner(this, executor, table, getContext(table));
  }
  
  public Scanner newScanner(final String table) {
    return newScanner(table.getBytes());
  }
  
  public short setFlushInterval(final short flush_interval) {
    // Note: if we have buffered increments, they'll pick up the new flush
    // interval next time the current timer fires.
    if (flush_interval < 0) {
      throw new IllegalArgumentException("Negative: " + flush_interval);
    }
    final short prev = config.flushInterval();
    config.overrideConfig("asynchbase.rpcs.buffered_flush_interval", 
        Short.toString(flush_interval));
    return prev;
  }
  
  public short getFlushInterval() {
    return config.flushInterval();
  }
  
  public Deferred<Object> shutdown() {
    try {
      // TODO - flag to prevent rpcs while shutting down
      for (final AstyanaxContext<Keyspace> context : contexts.values()) {
        context.shutdown();
      }
      executor.shutdown();
    } catch (Exception e) {
      LOG.error("failed to close the contexts", e);
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
  
  public Deferred<Object> flush() {
    num_flushes.incrementAndGet();
    return Deferred.fromResult(null);
  }

  public void overRideSocketTimeout(final int timeout) {
    // dunna care
  }
  
  public void logNSREBuffer(final boolean val) {
    // dunna care
  }
  
  public void logInflightBuffer(final boolean val) {
    // dunna care
  }
  
  public ByteMap<Integer> getNsreCounts() {
    return new ByteMap<Integer>();
  }
  
  /**
   * A no-op for BigTable clients
   * @param table Ignored
   * @return A deferred with a null result immediately.
   * @deprecated
   */
  public Deferred<Object> prefetchMeta(final String table) {
    return Deferred.fromResult(null);
  }
  
  /**
   * A no-op for BigTable clients
   * @param table Ignored
   * @param start Ignored
   * @param stop Ignored
   * @return A deferred with a null result immediately.
   * @deprecated
   */
  public Deferred<Object> prefetchMeta(final String table,
      final String start,
      final String stop) {
    return Deferred.fromResult(null);
  }
  
  /**
   * A no-op for BigTable clients
   * @param table Ignored
   * @return A deferred with a null result immediately.
   * @deprecated
   */
  public Deferred<Object> prefetchMeta(final byte[] table) {
    return Deferred.fromResult(null);
  }
  
  /**
   * A no-op for BigTable clients
   * @param table Ignored
   * @param start Ignored
   * @param stop Ignored
   * @return A deferred with a null result immediately.
   * @deprecated
   */
  public Deferred<Object> prefetchMeta(final byte[] table,
      final byte[] start,
      final byte[] stop) {
    return Deferred.fromResult(null);
  }
  
  
  private Keyspace getContext(final byte[] table) {
    Keyspace keyspace = keyspaces.get(table);
    if (keyspace == null) {
      synchronized (keyspaces) {
        // avoid race conditions where another thread put the client
        keyspace = keyspaces.get(table);
        AstyanaxContext<Keyspace> context = contexts.get(table);
        if (context != null) {
          LOG.warn("Context wasn't null for new keyspace " + Bytes.pretty(table));
        }
        context = new AstyanaxContext.Builder()
          .forCluster(config.getString("asynccassandra.cluster"))
          .forKeyspace(config.getString("asynccassandra.keyspace"))
          .withAstyanaxConfiguration(ast_config)
          .withConnectionPoolConfiguration(pool)
          .withConnectionPoolMonitor(monitor)
          .buildKeyspace(ThriftFamilyFactory.getInstance());
        contexts.put(table, context);
        context.start();

        keyspace = context.getClient();

        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutations.put(table, mutation);

        buffer_tracker.put(table, new AtomicLong());

        keyspaces.put(table, keyspace);
      }
    }
    return keyspace;
  }

  void incrementScans(){
    num_scans.incrementAndGet();
  }
  
  /**
   * Some arbitrary junk that is unlikely to appear in a real row key.
   * @see probeKey
   */
  static byte[] PROBE_SUFFIX = {
    ':', 'A', 's', 'y', 'n', 'c', 'H', 'B', 'a', 's', 'e',
    '~', 'p', 'r', 'o', 'b', 'e', '~', '<', ';', '_', '<',
  };
  
  /**
   * Returns a newly allocated key to probe, to check a region is online.
   * Sometimes we need to "poke" HBase to see if a region is online or a table
   * exists.  Given a key, we prepend some unique suffix to make it a lot less
   * likely that we hit a real key with our probe, as doing so might have some
   * implications on the RegionServer's memory usage.  Yes, some people with
   * very large keys were experiencing OOM's in their RegionServers due to
   * AsyncHBase probes.
   */
  private static byte[] probeKey(final byte[] key) {
    final byte[] testKey = new byte[key.length + 64];
    System.arraycopy(key, 0, testKey, 0, key.length);
    System.arraycopy(PROBE_SUFFIX, 0,
                     testKey, testKey.length - PROBE_SUFFIX.length,
                     PROBE_SUFFIX.length);
    return testKey;
  }
}

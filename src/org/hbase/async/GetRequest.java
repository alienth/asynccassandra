/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * Reads something from HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class GetRequest extends HBaseRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey,
             HBaseRpc.HasFamily, HBaseRpc.HasQualifiers, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GetRequest.class);
  
  private static final byte[] GET = new byte[] { 'g', 'e', 't' };
  static final byte[] GGET = new byte[] { 'G', 'e', 't' };  // HBase 0.95+
  private static final byte[] EXISTS =
    new byte[] { 'e', 'x', 'i', 's', 't', 's' };

  private byte[] family;     // TODO(tsuna): Handle multiple families?
  private byte[][] qualifiers;
  private Deferred<ArrayList<KeyValue>> deferred;

  /**
   * How many versions of each cell to retrieve.
   * The least significant bit is used as a flag to indicate when
   * this Get request is in fact an Exist request (i.e. like Get
   * except that instead of returning a result the RPC returns a
   * boolean indicating whether the row exists or not).
   */
  private int versions = 1 << 1;

  /** When set in the `versions' field, this is an Exist RPC. */
  private static final int EXIST_FLAG = 0x1;

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  public GetRequest(final byte[] table, final byte[] key) {
    super(table, key);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * <strong>This byte array will NOT be copied.</strong>
   */
  public GetRequest(final String table, final byte[] key) {
    this(table.getBytes(), key);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  public GetRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes());
  }

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family.
   * @since 1.5
   */
  public GetRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family) {
    super(table, key);
    this.family(family);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family.
   * @since 1.5
   */
  public GetRequest(final String table,
                    final String key,
                    final String family) {
    this(table, key);
    this.family(family);
  }

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family.
   * @param qualifier The column qualifier.
   * @since 1.5
   */
  public GetRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[] qualifier) {
    super(table, key);
    this.family(family);
    this.qualifier(qualifier);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family.
   * @param qualifier The column qualifier.
   * @since 1.5
   */
  public GetRequest(final String table,
                    final String key,
                    final String family,
                    final String qualifier) {
    this(table, key);
    this.family(family);
    this.qualifier(qualifier);
  }

  /**
   * Private constructor to build an "exists" RPC.
   * @param unused Unused, simply used to help the compiler find this ctor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  private GetRequest(final float unused,
                     final byte[] table,
                     final byte[] key) {
    super(table, key);
    this.versions |= EXIST_FLAG;
  }

  /**
   * Package-private factory method to build an "exists" RPC.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @return An {@link HBaseRpc} that will return a {@link Boolean}
   * indicating whether or not the given table / key exists.
   */
  static GetRequest exists(final byte[] table, final byte[] key) {
    return new GetRequest(0F, table, key);
  }

  /**
   * Package-private factory method to build an "exists" RPC.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family to get in the table.
   * @return An {@link HBaseRpc} that will return a {@link Boolean}
   * indicating whether or not the given table / key exists.
   */
  static GetRequest exists(final byte[] table,
                         final byte[] key, final byte[] family) {
    final GetRequest rpc = new GetRequest(0F, table, key);
    rpc.family(family);
    return rpc;
  }

  /** Returns true if this is actually an "Get" RPC. */
  private boolean isGetRequest() {
    return (versions & EXIST_FLAG) == 0;
  }

  /**
   * Specifies a particular column family to get.
   * @param family The column family.
   * <strong>This byte array will NOT be copied.</strong>
   * @return {@code this}, always.
   */
  public GetRequest family(final byte[] family) {
    KeyValue.checkFamily(family);
    this.family = family;
    return this;
  }

  /**
   * Specifies a particular column family to get.
   * @param family The column family.
   * @return {@code this}, always.
   */
  public GetRequest family(final String family) {
    return family(family.getBytes());
  }

  /**
   * Specifies a particular column qualifier to get.
   * @param qualifier The column qualifier.
   * <strong>This byte array will NOT be copied.</strong>
   * @return {@code this}, always.
   */
  public GetRequest qualifier(final byte[] qualifier) {
    if (qualifier == null) {
      throw new NullPointerException("qualifier");
    }
    KeyValue.checkQualifier(qualifier);
    this.qualifiers = new byte[][] { qualifier };
    return this;
  }

  /**
   * Specifies a particular set of column qualifiers to get.
   * @param qualifiers The column qualifiers.
   * <strong>This byte array will NOT be copied.</strong>
   * @return {@code this}, always.
   * @since 1.1
   */
  public GetRequest qualifiers(final byte[][] qualifiers) {
    if (qualifiers == null) {
      throw new NullPointerException("qualifiers");
    }
    for (final byte[] qualifier : qualifiers) {
      KeyValue.checkQualifier(qualifier);
    }
    this.qualifiers = qualifiers;
    return this;
  }

  /**
   * Specifies a particular column qualifier to get.
   * @param qualifier The column qualifier.
   * @return {@code this}, always.
   */
  public GetRequest qualifier(final String qualifier) {
    return qualifier(qualifier.getBytes());
  }

  

  /**
   * Sets the maximum number of versions to return for each cell read.
   * <p>
   * By default only the most recent version of each cell is read.
   * If you want to get all possible versions available, pass
   * {@link Integer#MAX_VALUE} in argument.
   * @param versions A strictly positive number of versions to return.
   * @return {@code this}, always.
   * @since 1.4
   * @throws IllegalArgumentException if {@code versions <= 0}
   */
  public GetRequest maxVersions(final int versions) {
    if (versions <= 0) {
      throw new IllegalArgumentException("Need a strictly positive number: "
                                         + versions);
    }
    this.versions = (versions << 1) | (this.versions & EXIST_FLAG);
    return this;
  }

  /**
   * Returns the maximum number of versions to return for each cell scanned.
   * @return A strictly positive integer.
   * @since 1.4
   */
  public int maxVersions() {
    return versions >>> 1;
  }

//  @Override
//  byte[] method(final byte server_version) {
//    if (server_version >= RegionClient.SERVER_VERSION_095_OR_ABOVE) {
//      return GGET;
//    }
//    return isGetRequest() ? GET : EXISTS;
//  }

  @Override
  public byte[] table() {
    return table;
  }

  @Override
  public byte[] key() {
    return key;
  }

  @Override
  public byte[] family() {
    return family;
  }

  @Override
  public byte[][] qualifiers() {
    return qualifiers;
  }

  public String toString() {
    final String klass = isGetRequest() ? "GetRequest" : "Exists";
    return super.toStringWithQualifiers(klass, family, qualifiers);
  }

  
  @Override
  ChannelBuffer serialize(byte server_version) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  Object deserialize(ChannelBuffer buf, int cell_size) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  byte[] method(byte server_version) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void run() {
    // TODO Auto-generated method stub
    
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //
//
//  /**
//   * Predicts a lower bound on the serialized size of this RPC.
//   * This is to avoid using a dynamic buffer, to avoid re-sizing the buffer.
//   * Since we use a static buffer, if the prediction is wrong and turns out
//   * to be less than what we need, there will be an exception which will
//   * prevent the RPC from being serialized.  That'd be a severe bug.
//   */
//  private int predictSerializedSize(final byte server_version) {
//    int size = 0;
//    size += 4;  // int:  Number of parameters.
//    size += 1;  // byte: Type of the 1st parameter.
//    size += 3;  // vint: region name length (3 bytes => max length = 32768).
//    size += region.name().length;  // The region name.
//
//    size += 1;  // byte: Type of the 2nd parameter.
//    size += 1;  // byte: Type again (see HBASE-2877).
//    size += 1;  // byte: Version of Get.
//    size += 3;  // vint: row key length (3 bytes => max length = 32768).
//    size += key.length;  // The row key.
//    size += 8;  // long: Lock ID.
//    size += 4;  // int:  Max number of versions to return.
//    size += 1;  // byte: Whether or not to use a filter.
//    if (server_version >= 26) {  // New in 0.90 (because of HBASE-3174).
//      size += 1;  // byte: Whether or not to cache the blocks read.
//    }
//    size += 8;  // long: Minimum timestamp.
//    size += 8;  // long: Maximum timestamp.
//    size += 1;  // byte: Boolean: "all time".
//    size += 4;  // int:  Number of families.
//    if (family != null) {
//      size += 1;  // vint: Family length (guaranteed on 1 byte).
//      size += family.length;  // The family.
//      size += 1;  // byte: Boolean: do we want specific qualifiers?
//      if (qualifiers != null) {
//        size += 4;  // int:  How many qualifiers follow?
//        for (final byte[] qualifier : qualifiers) {
//          size += 3;  // vint: Qualifier length.
//          size += qualifier.length;  // The qualifier.
//        }
//      }
//    }
//    if (server_version >= RegionClient.SERVER_VERSION_092_OR_ABOVE) {
//      size += 4;  // int: Attributes map.  Always 0.
//    }
//    return size;
//  }
//
//  /** Serializes this request.  */
//  ChannelBuffer serialize(final byte server_version) {
//    if (server_version < RegionClient.SERVER_VERSION_095_OR_ABOVE) {
//      return serializeOld(server_version);
//    }
//    final ClientPB.Get.Builder getpb = ClientPB.Get.newBuilder()
//      .setRow(Bytes.wrap(key));
//
//    if (family != null) {
//      final ClientPB.Column.Builder column = ClientPB.Column.newBuilder();
//      column.setFamily(Bytes.wrap(family));
//      if (qualifiers != null) {
//        for (final byte[] qualifier : qualifiers) {
//          column.addQualifier(Bytes.wrap(qualifier));
//        }
//      }
//      getpb.addColumn(column.build());
//    }
//
//    // TODO: Filters.
//
//    final int versions = maxVersions();  // Shadows this.versions
//    if (versions != 1) {
//      getpb.setMaxVersions(versions);
//    }
//    if (!isGetRequest()) {
//      getpb.setExistenceOnly(true);
//    }
//
//    final ClientPB.GetRequest.Builder get = ClientPB.GetRequest.newBuilder()
//      .setRegion(region.toProtobuf())
//      .setGet(getpb.build());
//
//    return toChannelBuffer(GetRequest.GGET, get.build());
//  }
//
//  /** Serializes this request for HBase 0.94 and before.  */
//  private ChannelBuffer serializeOld(final byte server_version) {
//    final ChannelBuffer buf = newBuffer(server_version,
//                                        predictSerializedSize(server_version));
//    buf.writeInt(2);  // Number of parameters.
//
//    // 1st param: byte array containing region name
//    writeHBaseByteArray(buf, region.name());
//
//    // 2nd param: Get object
//    buf.writeByte(32);   // Code for a `Get' parameter.
//    buf.writeByte(32);   // Code again (see HBASE-2877).
//    buf.writeByte(1);    // Get#GET_VERSION.  Undocumented versioning of Get.
//    writeByteArray(buf, key);
//    buf.writeLong(lockid);  // Lock ID.
//    buf.writeInt(maxVersions()); // Max number of versions to return.
//    buf.writeByte(0x00); // boolean (false): whether or not to use a filter.
//    // If the previous boolean was true:
//    //   writeByteArray(buf, filter name as byte array);
//    //   write the filter itself
//
//    if (server_version >= 26) {  // New in 0.90 (because of HBASE-3174).
//      buf.writeByte(0x01);  // boolean (true): whether to cache the blocks.
//    }
//
//    // TimeRange
//    buf.writeLong(0);               // Minimum timestamp.
//    buf.writeLong(Long.MAX_VALUE);  // Maximum timestamp.
//    buf.writeByte(0x01);            // Boolean: "all time".
//    // The "all time" boolean indicates whether or not this time range covers
//    // all possible times.  Not sure why it's part of the serialized RPC...
//
//    // Families.
//    buf.writeInt(family != null ? 1 : 0);  // Number of families that follow.
//
//    if (family != null) {
//      // Each family is then written like so:
//      writeByteArray(buf, family);  // Column family name.
//      if (qualifiers != null) {
//        buf.writeByte(0x01);  // Boolean: We want specific qualifiers.
//        buf.writeInt(qualifiers.length);   // How many qualifiers do we want?
//        for (final byte[] qualifier : qualifiers) {
//          writeByteArray(buf, qualifier);  // Column qualifier name.
//        }
//      } else {
//        buf.writeByte(0x00);  // Boolean: we don't want specific qualifiers.
//      }
//    }
//    if (server_version >= RegionClient.SERVER_VERSION_092_OR_ABOVE) {
//      buf.writeInt(0);  // Attributes map: number of elements.
//    }
//    return buf;
//  }
//
//  @Override
//  Object deserialize(final ChannelBuffer buf, final int cell_size) {
//    final ClientPB.GetResponse resp =
//      readProtobuf(buf, ClientPB.GetResponse.PARSER);
//    if (isGetRequest()) {
//      return extractResponse(resp, buf, cell_size);
//    } else {
//      final ClientPB.Result result = resp.getResult();
//      return result != null ? result.getExists() : false;  // is `null' possible here?
//    }
//  }
//
//  /**
//   * Transforms a protobuf get response into a list of {@link KeyValue}.
//   * @param resp The protobuf response from which to extract the KVs.
//   * @param buf The buffer from which the protobuf was read.
//   * @param cell_size The number of bytes of the cell block that follows,
//   * in the buffer.
//   */
//  static ArrayList<KeyValue> extractResponse(final ClientPB.GetResponse resp,
//                                             final ChannelBuffer buf,
//                                             final int cell_size) {
//    final ClientPB.Result res = resp.getResult();
//    if (res == null) {
//      return new ArrayList<KeyValue>(0);
//    }
//    return convertResult(res, buf, cell_size);
//  }
//

}

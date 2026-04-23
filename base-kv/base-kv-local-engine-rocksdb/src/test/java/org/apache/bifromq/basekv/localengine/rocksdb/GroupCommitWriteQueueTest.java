/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.google.protobuf.ByteString;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.localengine.MockableTest;
import org.apache.bifromq.basekv.localengine.TestUtil;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.testng.annotations.Test;

public class GroupCommitWriteQueueTest extends MockableTest {
    static {
        RocksDB.loadLibrary();
    }

    private Path dbRootDir;
    private Options options;
    private RocksDB db;
    private ColumnFamilyHandle cfHandle;
    private WriteOptions writeOptions;
    private GroupCommitWriteQueue queue;

    @SneakyThrows
    @Override
    protected void doSetup(Method method) {
        super.doSetup(method);
        dbRootDir = Files.createTempDirectory("");
        options = new Options().setCreateIfMissing(true);
        db = RocksDB.open(options, dbRootDir.toAbsolutePath().toString());
        cfHandle = db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));
        writeOptions = new WriteOptions();
        queue = new GroupCommitWriteQueue(db, writeOptions);
    }

    @Override
    protected void doTeardown(Method method) {
        super.doTeardown(method);
        if (cfHandle != null) {
            cfHandle.close();
        }
        if (db != null) {
            db.close();
        }
        if (options != null) {
            options.close();
        }
        if (writeOptions != null) {
            writeOptions.close();
        }
        TestUtil.deleteDir(dbRootDir.toString());
    }

    @Test
    public void testSingleBatch() throws RocksDBException {
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(cfHandle, "key1".getBytes(), "value1".getBytes());
            queue.submit(batch);
        }
        assertEquals(new String(db.get(cfHandle, "key1".getBytes())), "value1");
    }

    @Test
    public void testMultipleBatchesMerged() throws Exception {
        int batchCount = 10;
        for (int i = 0; i < batchCount; i++) {
            try (WriteBatch batch = new WriteBatch()) {
                batch.put(cfHandle, ("key" + i).getBytes(), ("value" + i).getBytes());
                queue.submit(batch);
            }
        }
        for (int i = 0; i < batchCount; i++) {
            assertEquals(new String(db.get(cfHandle, ("key" + i).getBytes())), "value" + i);
        }
    }

    @Test
    public void testConcurrentBatches() throws Exception {
        int threadCount = 8;
        int batchesPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    for (int i = 0; i < batchesPerThread; i++) {
                        try (WriteBatch batch = new WriteBatch()) {
                            String key = "t" + threadId + "k" + i;
                            String value = "t" + threadId + "v" + i;
                            batch.put(cfHandle, key.getBytes(), value.getBytes());
                            queue.submit(batch);
                        } catch (RocksDBException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }));
        }

        latch.await();
        for (Future<?> future : futures) {
            future.get();
        }
        executor.shutdown();

        for (int t = 0; t < threadCount; t++) {
            for (int i = 0; i < batchesPerThread; i++) {
                String key = "t" + t + "k" + i;
                String value = "t" + t + "v" + i;
                assertEquals(new String(db.get(cfHandle, key.getBytes())), value);
            }
        }
    }

    @Test
    public void testDeleteAndDeleteRangeMerged() throws Exception {
        try (WriteBatch batch = new WriteBatch()) {
            batch.put(cfHandle, "delKey".getBytes(), "delValue".getBytes());
            batch.put(cfHandle, "rangeKey1".getBytes(), "rangeValue1".getBytes());
            batch.put(cfHandle, "rangeKey2".getBytes(), "rangeValue2".getBytes());
            queue.submit(batch);
        }

        try (WriteBatch batch = new WriteBatch()) {
            batch.singleDelete(cfHandle, "delKey".getBytes());
            batch.deleteRange(cfHandle, "rangeKey1".getBytes(), "rangeKey3".getBytes());
            queue.submit(batch);
        }

        assertNull(db.get(cfHandle, "delKey".getBytes()));
        assertNull(db.get(cfHandle, "rangeKey1".getBytes()));
        assertNull(db.get(cfHandle, "rangeKey2".getBytes()));
    }
}

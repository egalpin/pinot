/**
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
package org.apache.pinot.perf;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 0, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Benchmark)
public class BenchmarkMapDb {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkMapDb.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkMapDb.class);
  private static final File PERSIST_DIR =
      new File(FileUtils.getTempDirectory(), BenchmarkMapDb.class.getSimpleName() + ".db");
  private BTreeMap<Integer, Integer> _map;
  private DB _db;
  private DB.TreeMapSink<Integer, Integer> _sink;


  @Param("100000")
  private int _numKeysToUpdate;
  @Param({"10000000", "1000000000"})
  long _numSeedKeys;
//  private Env<DirectBuffer> env;

  @Setup
  public void setUp()
      throws Exception {

    FileUtils.deleteQuietly(PERSIST_DIR);
    LOGGER.info("Initialize map");
    _db = DBMaker
        .fileDB(PERSIST_DIR)
        .fileMmapEnable()
        .fileMmapPreclearDisable()
        .fileSyncDisable()
        .closeOnJvmShutdown()
        .cleanerHackEnable()
        .make();

    _db.getStore().fileLoad();

    _sink = _db
        .treeMap("map", Serializer.INTEGER,Serializer.INTEGER)
        .createFromSink();

    addSeedKeys();
    LOGGER.info("Done initializing map");
  }

  public void addSeedKeys() {
    LOGGER.info("Start adding keys");
    for (int i = 0; i < _numSeedKeys; i++) {
      _sink.put(i, i);
    }

    _map = _sink.create();
    LOGGER.info("Done adding keys");
  }

  @Benchmark
  public void modifyKeys() {
    for (int i = 0; i < _numKeysToUpdate; i++) {
      _map.get(i);
      _map.put(i, i + 1);
    }
  }

  @TearDown
  public void tearDown() {
    LOGGER.info("Start cleaning up");
    _db.close();
    FileUtils.deleteQuietly(PERSIST_DIR);
    LOGGER.info("Done cleaning up");
  }
}

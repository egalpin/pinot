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
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Values;
import org.apache.commons.io.FileUtils;
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


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 0, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Benchmark)
public class BenchmarkChronicleMap {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkChronicleMap.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  private static final File PERSIST_DIR =
      new File(FileUtils.getTempDirectory(), BenchmarkChronicleMap.class.getSimpleName() + ".dat");

  @Param("1000000")
  private int _numKeysToUpdate;
  @Param({"1000000", "10000000", "100000000"})
  long _numSeedKeys;
  @Param({"0.5", "1.0", "1.5", "2.0"})
  float _extraSpaceRatio;
  @Param({"true", "false"})
  boolean _isPersistedToDisk;
  private ChronicleMap<LongValue, LongValue> _primaryKeyToRecordLocationMap;
//  StringBuilder key = new StringBuilder();
  LongValue _key = Values.newHeapInstance(LongValue.class);
  LongValue _value = Values.newHeapInstance(LongValue.class);

  @Setup
  public void setUp()
      throws Exception {
    ChronicleMapBuilder<LongValue, LongValue> builder = ChronicleMap
        .of(LongValue.class, LongValue.class)
        .name("_primaryKeyToRecordLocationMap")
        .entries((long) Math.ceil(
            (_numKeysToUpdate + _numSeedKeys) * _extraSpaceRatio
        ))
        .constantKeySizeBySample(_key)
        .putReturnsNull(true)
        .removeReturnsNull(true)
        .constantValueSizeBySample(_value);
    if (_isPersistedToDisk) {
      _primaryKeyToRecordLocationMap = builder.createPersistedTo(PERSIST_DIR);
    } else {
      _primaryKeyToRecordLocationMap = builder.create();
    }

//    addSeedKeys();
  }

  @Benchmark
  public void addSeedKeys() {
    for (long i = 0; i <= _numSeedKeys; i++) {
      _key.setValue(i);
      _value.setValue(i);
      _primaryKeyToRecordLocationMap.put(_key, _value);
    }
  }

  @TearDown
  public void tearDown() {
    _primaryKeyToRecordLocationMap.close();
    FileUtils.deleteQuietly(PERSIST_DIR);
  }

//  @Benchmark
//  public void doUpdate() {
//    for (long i = 0; i <= _numKeysToUpdate; i++) {
//      _key.setValue(i);
//      _value.setValue(i + 1);
//      _primaryKeyToRecordLocationMap.put(_key, _value);
//    }
//  }
}

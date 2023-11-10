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
package org.apache.pinot.common.config.provider;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class LogicalTable {
  private List<TableConfig> _offlineTables;
  private List<TableConfig> _realtimeTables;
  private List<Pair<TableConfig, TableConfig>> _hybridTables;

  public LogicalTable() {
    _offlineTables = new ArrayList<>();
    _realtimeTables = new ArrayList<>();
    _hybridTables = new ArrayList<>();
  }

  public LogicalTable(List<TableConfig> offlineTables, List<TableConfig> realtimeTables,
      List<Pair<TableConfig, TableConfig>> hybridTables) {
    _offlineTables = offlineTables;
    _realtimeTables = realtimeTables;
    _hybridTables = hybridTables;
  }

  public void addAll(LogicalTable other) {
    _offlineTables.addAll(other.getOfflineTables());
    _realtimeTables.addAll(other.getRealtimeTables());
    _hybridTables.addAll(other.getHybridTables());
  }

  public void addOfflineTable(TableConfig tableConfig) {
    _offlineTables.add(tableConfig);
  }

  public void addRealtimeTable(TableConfig tableConfig) {
    _realtimeTables.add(tableConfig);
  }

  public void addHybridTable(TableConfig offlineTableConfig, TableConfig realtimeTableConfig) {
    String rawOfflineTableName = TableNameBuilder.extractRawTableName(offlineTableConfig.getTableName());
    String rawRealtimeTableName = TableNameBuilder.extractRawTableName(realtimeTableConfig.getTableName());
    Preconditions.checkArgument(rawOfflineTableName.equals(rawRealtimeTableName),
        "Raw OFFLINE table name \"%s\" does not match raw REALTIME table name \"%s\". Table cannot be hybrid.",
        offlineTableConfig.getTableName(), realtimeTableConfig.getTableName());

    _hybridTables.add(Pair.of(offlineTableConfig, realtimeTableConfig));
  }

  public List<TableConfig> getOfflineTables() {
    return _offlineTables;
  }

  public List<TableConfig> getRealtimeTables() {
    return _realtimeTables;
  }

  public List<Pair<TableConfig, TableConfig>> getHybridTables() {
    return _hybridTables;
  }

  public void setOfflineTables(List<TableConfig> offlineTables) {
    _offlineTables = offlineTables;
  }

  public void setRealtimeTables(List<TableConfig> realtimeTables) {
    _realtimeTables = realtimeTables;
  }

  public void setHybridTables(List<Pair<TableConfig, TableConfig>> hybridTables) {
    _hybridTables = hybridTables;
  }
}

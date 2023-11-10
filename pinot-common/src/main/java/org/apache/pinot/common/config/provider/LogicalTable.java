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

import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;


public class LogicalTable {
  private List<TableConfig> _offlineTables;
  private List<TableConfig> _realtimeTables;
  private List<TableConfig> _hybridTables;

  public LogicalTable(List<TableConfig> offlineTables, List<TableConfig> realtimeTables, List<TableConfig> hybridTables) {
    _offlineTables = offlineTables;
    _realtimeTables = realtimeTables;
    _hybridTables = hybridTables;
  }

  public List<TableConfig> getOfflineTables() {
    return _offlineTables;
  }

  public List<TableConfig> getRealtimeTables() {
    return _realtimeTables;
  }

  public List<TableConfig> getHybridTables() {
    return _hybridTables;
  }
}

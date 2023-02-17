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
package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


@SuppressWarnings({"rawtypes", "unchecked"})
public class ComparisonColumns implements Comparable {
  private Map<String, ComparisonValue> _comparisonColumns;

  public ComparisonColumns(Map<String, ComparisonValue> comparisonColumns) {
    _comparisonColumns = comparisonColumns;
  }

  public Map<String, ComparisonValue> getComparisonColumns() {
    return _comparisonColumns;
  }

  @Override
  public int compareTo(@Nonnull Object other) {
    if (other instanceof ComparisonColumns) {
      return compareToComparisonColumns((ComparisonColumns) other);
    }

    // If other is not an instance of ComparisonColumns, it must be the case that the upsert config has been updated
    // since last server restart.
    //
    // In the case where the upsert config is edited between restarts, the first comparison for an existing row will
    // end up comparing a value from the _new_ column against the previously stored Comparable from the _previous_
    // column. The same functionality can be used here, where the previously stored Comparable will be compared
    // against the non-null ComparisonColumn value.
    return compareToComparable(other);
  }

  private int compareToComparable(@Nonnull Object other) {

    for (Map.Entry<String, ComparisonValue> columnEntry : _comparisonColumns.entrySet()) {
      ComparisonValue comparisonValue = columnEntry.getValue();
      if (!comparisonValue.isNull()) {
        return comparisonValue.compareTo(other);
      }
    }
    return -1;
  }

  private int compareToComparisonColumns(@Nonnull ComparisonColumns other) {
    for (Map.Entry<String, ComparisonValue> columnEntry : _comparisonColumns.entrySet()) {
      ComparisonValue comparisonValue = columnEntry.getValue();
      // Inbound records may have at most 1 non-null value. _other may have all non-null values, however.
      if (comparisonValue.isNull()) {
        continue;
      }

      ComparisonValue otherComparisonValue = other.getComparisonColumns().get(columnEntry.getKey());

      if (otherComparisonValue == null) {
        // This can happen if a new column is added to the list of comparisonColumns. We want to support that without
        // requiring a server restart, so handle the null here.
        _comparisonColumns = merge(other.getComparisonColumns(), _comparisonColumns);
        return 1;
      }

      int comparisonResult = comparisonValue.compareTo(otherComparisonValue);
      if (comparisonResult >= 0) {
        _comparisonColumns = merge(other.getComparisonColumns(), _comparisonColumns);
        return comparisonResult;
      }
    }

    // note that we will reach here if all comparison values are null
    return -1;
  }

  private static Map<String, ComparisonValue> merge(@Nullable Map<String, ComparisonValue> current,
      @Nonnull Map<String, ComparisonValue> next) {
    // merge the values of this new row with the comparison values from any previous upsert. This should only be
    // called in the case where next.compareTo(current) >= 0
    if (current == null) {
      return next;
    }

    // Create a shallow copy so {@param current} is unmodified
    Map<String, ComparisonValue> mergedComparisonColumns = new HashMap<>(current);

    for (Map.Entry<String, ComparisonValue> columnEntry : next.entrySet()) {
      ComparisonValue inboundValue = columnEntry.getValue();
      String columnName = columnEntry.getKey();
      ComparisonValue existingValue = mergedComparisonColumns.get(columnName);

      if (existingValue == null) {
        mergedComparisonColumns.put(columnName,
            new ComparisonValue(inboundValue.getComparisonValue(), inboundValue.isNull()));
        continue;
      }

      int comparisonResult = inboundValue.compareTo(existingValue);
      Comparable comparisonValue =
          comparisonResult >= 0 ? inboundValue.getComparisonValue() : existingValue.getComparisonValue();

      mergedComparisonColumns.put(columnName, new ComparisonValue(comparisonValue));
    }
    return mergedComparisonColumns;
  }
}

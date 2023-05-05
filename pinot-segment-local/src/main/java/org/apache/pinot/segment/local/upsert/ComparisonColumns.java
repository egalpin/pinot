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


@SuppressWarnings({"rawtypes", "unchecked"})
public class ComparisonColumns implements Comparable<ComparisonColumns> {
  private final Comparable[] _values;
  private final int _comparableIndex;
  public static final int SEALED_SEGMENT_COMPARISON_INDEX = -1;

  public ComparisonColumns(Comparable[] values, int comparableIndex) {
    _values = values;
    _comparableIndex = comparableIndex;
  }

  public Comparable[] getValues() {
    return _values;
  }

  public int getComparableIndex() {
    return _comparableIndex;
  }

  public int compareToSealed(ComparisonColumns other) {
      /*
       - iterate over all columns
       - if any value in _values is greater than its counterpart in _other._values, keep _values as-is and return 1
       - if any value in _values is less than its counterpart  in _other._values _and_ none are greater than their
         counterpart in _other._values, keep _values as-is and return -1
           - i.e. no value in _this_ is greater than its counterpart in _other
       - if all values between the two sets of Comparables are equal (compareTo == 0), keep _values as-is and return 0
       */
    int comparisonResult;
    int accumResult = 0;

    for (int i = 0; i < _values.length; i++) {
      Comparable comparisonValue = _values[i];
      Comparable otherComparisonValue = other.getValues()[i];
      if (otherComparisonValue == null) {
        continue;
      } else {
        comparisonResult = comparisonValue.compareTo(otherComparisonValue);
      }

      if (comparisonResult > 0) {
        return 1;
      } else {
        accumResult += comparisonResult;
      }
    }
    return Math.max(accumResult, -1);
  }

  @Override
  public int compareTo(ComparisonColumns other) {
    if (_comparableIndex == SEALED_SEGMENT_COMPARISON_INDEX) {
      return compareToSealed(other);
    }

    // _comparisonColumns should only at most one non-null comparison value for newly ingested data. If not, it is
    // the user's responsibility. There is no attempt to guarantee behavior in the case where there are multiple
    // non-null values
    int comparisonResult;

    Comparable comparisonValue = _values[_comparableIndex];
    Comparable otherComparisonValue = other.getValues()[_comparableIndex];

    if (otherComparisonValue == null) {
      // Keep this record because the existing record has no value for the same comparison column, therefore the
      // (lack of) existing value could not possibly cause the new value to be rejected.
      comparisonResult = 1;
    } else {
      comparisonResult = comparisonValue.compareTo(otherComparisonValue);
    }

    if (comparisonResult >= 0) {
      // TODO(egalpin):  This method currently may have side-effects on _values. Depending on the comparison result,
      //  entities from {@param other} may be merged into _values. This really should not be done implicitly as part
      //  of compareTo, but has been implemented this way to minimize the changes required within all subclasses of
      //  {@link BasePartitionUpsertMetadataManager}. Ideally, this merge should only be triggered explicitly by
      //  implementations of {@link BasePartitionUpsertMetadataManager}.
      for (int i = 0; i < _values.length; i++) {
        if (i != _comparableIndex) {
          _values[i] = other._values[i];
        }
      }
    }
    return comparisonResult;
  }
}

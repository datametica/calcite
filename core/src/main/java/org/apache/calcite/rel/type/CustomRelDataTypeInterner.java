/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.type;

import com.google.common.collect.Interner;
import com.google.common.collect.MapMaker;

import org.apache.calcite.sql.type.ObjectSqlType;

import java.util.concurrent.ConcurrentMap;

public class CustomRelDataTypeInterner implements Interner<RelDataType> {

    private final ConcurrentMap<RelDataType, Object> map =
            new MapMaker().weakKeys().makeMap();

    private static final Object VALUE = new Object(); // Simulate Dummy.VALUE

    @Override
    public RelDataType intern(RelDataType sample) {
        Object sneaky;
        do {
            // Simulate map.getEntry(sample) by doing a get()
            // (Since getEntry is not publicly accessible, this is the best we can do)
            RelDataType canonical = getCanonical(sample);
            if (canonical != null) {
                return canonical;
            }

            // Try inserting this sample
            sneaky = map.putIfAbsent(sample, VALUE);

        } while (sneaky != null); // If already present, retry

        return sample; // First successful insert — now canonical
    }

  private RelDataType getCanonical(RelDataType sample) {
    for (RelDataType key : map.keySet()) {
      if (key.equals(sample)) {
        if (sample instanceof ObjectSqlType) {
          ObjectSqlType objectSqlType = (ObjectSqlType) sample;
          ObjectSqlType objectSqlTypeKey = (ObjectSqlType) key;
          if (objectSqlTypeKey.fieldList.hashCode() == objectSqlType.fieldList.hashCode()) {
            return key;
          }
        }
      }
    }
    return null;
  }
}

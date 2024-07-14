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
package org.apache.calcite.adapter.os;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Table function that executes the OS "cpu_info".
 */
public class CpuTimeTableFunction {
  private CpuTimeTableFunction() {
  }

  public static ScannableTable eval(boolean b) {
    return new AbstractBaseScannableTable() {
      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
          @Override public Enumerator<Object[]> enumerator() {
            return new OsQuery("cpu_time");
          }
        };
      }

      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("idle", SqlTypeName.BIGINT)
            .add("nice", SqlTypeName.BIGINT)
            .add("irq", SqlTypeName.BIGINT)
            .add("soft_irq", SqlTypeName.BIGINT)
            .add("steal", SqlTypeName.BIGINT)
            .add("system", SqlTypeName.BIGINT)
            .add("user", SqlTypeName.BIGINT)
            .add("io_wait", SqlTypeName.BIGINT)
            .build();
      }
    };
  }
}

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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A <code>SqlDialect</code> implementation for the Trino database.
 */
public class TrinoSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.PRESTO)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.UNCHANGED)
      .withNullCollation(NullCollation.LAST);

  public static final SqlDialect DEFAULT = new TrinoSqlDialect(DEFAULT_CONTEXT);

  /**
   * Creates a PrestoSqlDialect.
   */
  public TrinoSqlDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean supportsGroupByWithCube() {
    return true;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsGroupByWithRollup() {
    return true;
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    return super.getCastSpec(type);
  }
}

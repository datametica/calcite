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
package org.apache.calcite.util;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.util.Objects;

/**
 * BigQuery-specific utilities for unparsing the {@code TO_NUMBER} function.
 */
public class BQToNumberUtils extends ToNumberUtils {

  private static final BigInteger INT32_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
  private static final BigInteger INT32_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
  private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);

  private BQToNumberUtils() {
  }

  /** Unparses TO_NUMBER for BigQuery with precision-aware CAST target types. */
  public static void unparseToNumber(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect dialect) {
    unparseToNumberAsCast(writer, call, leftPrec, rightPrec, dialect,
        BQToNumberUtils::resolveBigQueryCastSpec);
  }

  /**
   * Resolves the CAST target type spec that {@link #unparseToNumber} would use for a
   * {@code TO_NUMBER} call. Used to detect redundant {@code CAST(TO_NUMBER(...) AS T)} wrappers.
   */
  public static SqlNode resolveCastSpecForCall(SqlCall call, SqlDialect dialect) {
    return resolveBigQueryCastSpec(call, dialect);
  }

  private static SqlNode resolveBigQueryCastSpec(SqlCall call, SqlDialect dialect) {
    if (!(call.operand(0) instanceof SqlCharStringLiteral)) {
      return castSpecForType(dialect, SqlTypeName.BIGINT);
    }
    String value = ((SqlCharStringLiteral) call.operand(0)).getValueAs(String.class);
    return resolveBigQueryCastSpecFromValue(value, dialect);
  }

  private static SqlNode resolveBigQueryCastSpecFromValue(String value, SqlDialect dialect) {
    if (isScientificNotation(value)) {
      return castSpecForType(dialect, SqlTypeName.DECIMAL);
    }
    NumericParts parts = parseNumericParts(value);
    if (parts.scale == 0) {
      BigInteger integerValue = parts.toBigInteger();
      if (integerValue != null) {
        if (integerValue.compareTo(INT32_MIN) >= 0
            && integerValue.compareTo(INT32_MAX) <= 0) {
          return castSpecForType(dialect, SqlTypeName.INTEGER);
        }
        if (integerValue.compareTo(INT64_MIN) >= 0
            && integerValue.compareTo(INT64_MAX) <= 0) {
          return castSpecForType(dialect, SqlTypeName.BIGINT);
        }
      }
    }
    if (parts.precision > 76 || parts.scale > 38) {
      return castSpecForType(dialect, SqlTypeName.DOUBLE);
    }
    RelDataType decimalType =
        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL,
            parts.precision, parts.scale);
    // BigQuery does not allow parameterized types in CAST expressions; use NUMERIC or
    // BIGNUMERIC without precision and scale.
    return Objects.requireNonNull(dialect.getCastSpec(decimalType));
  }

  private static boolean isScientificNotation(String value) {
    int eIndex = value.toUpperCase().lastIndexOf('E');
    return eIndex > 0 && eIndex < value.length() - 1;
  }

  private static NumericParts parseNumericParts(String value) {
    String trimmed = value.trim();
    boolean negative = trimmed.startsWith("-");
    if (negative || trimmed.startsWith("+")) {
      trimmed = trimmed.substring(1);
    }
    int dotIndex = trimmed.indexOf('.');
    int integerDigits;
    int scale;
    if (dotIndex >= 0) {
      integerDigits = countDigits(trimmed.substring(0, dotIndex));
      scale = trimmed.substring(dotIndex + 1).length();
    } else {
      integerDigits = countDigits(trimmed);
      scale = 0;
    }
    int precision = scale > 0 ? integerDigits + scale : integerDigits;
    return new NumericParts(trimmed, negative, scale, precision);
  }

  private static int countDigits(String s) {
    int count = 0;
    for (int i = 0; i < s.length(); i++) {
      if (Character.isDigit(s.charAt(i))) {
        count++;
      }
    }
    return count;
  }

  /** Parsed numeric components of a TO_NUMBER operand literal. */
  private static final class NumericParts {
    private final String unsignedValue;
    private final boolean negative;
    private final int scale;
    private final int precision;

    private NumericParts(String unsignedValue, boolean negative, int scale, int precision) {
      this.unsignedValue = unsignedValue;
      this.negative = negative;
      this.scale = scale;
      this.precision = precision;
    }

    private @Nullable BigInteger toBigInteger() {
      if (scale > 0) {
        return null;
      }
      try {
        String digits = unsignedValue.replaceAll("[^0-9]", "");
        if (digits.isEmpty()) {
          return BigInteger.ZERO;
        }
        BigInteger result = new BigInteger(digits);
        return negative ? result.negate() : result;
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }
}

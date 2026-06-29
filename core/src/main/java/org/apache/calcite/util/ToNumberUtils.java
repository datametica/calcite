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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * This class is specific to BigQuery, Hive, Spark and Snowflake.
 */
public class ToNumberUtils {

  private static final String REGEX_REMOVE = "[',$A-Za-z]+";
  private static final Pattern HEX_FORMAT_PATTERN = Pattern.compile("^'[Xx]+'$");

  private static final BigInteger INT32_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
  private static final BigInteger INT32_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
  private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);

  private ToNumberUtils() {
  }

  /** Unparses TO_NUMBER for BigQuery with precision-aware CAST target types. */
  public static void unparseToNumberBigQuery(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect dialect) {
    unparseToNumberAsCast(writer, call, leftPrec, rightPrec, dialect,
        ToNumberUtils::resolveBigQueryCastSpec);
  }

  public static void unparseToNumber(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect dialect) {
    unparseToNumberAsCast(writer, call, leftPrec, rightPrec, dialect,
        ToNumberUtils::resolveDefaultCastSpec);
  }

  public static void unparseToNumberSnowFlake(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getOperandList().size()) {
    case 1:
    case 3:
      parseToNumber(writer, leftPrec, rightPrec, prepareSqlNodes(call));
      break;
    case 2:
      if (isFirstOperandCurrencyType(call)) {
        String secondOperand = call.operand(1).toString().replaceAll("[UL]", "\\$")
            .replace("'", "");
        parseToNumber(writer, leftPrec, rightPrec,
            new SqlNode[]{call.operand(0),
                SqlLiteral.createCharString(secondOperand.trim(), SqlParserPos.ZERO)});
      } else if (isOperandNull(call)) {
        parseToNumber(writer, leftPrec, rightPrec,
            new SqlNode[]{new SqlDataTypeSpec(
                new SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
                SqlParserPos.ZERO)});
      } else if (isOperandTypeOfCurrencyOrContainSpace(call)) {
        parseToNumber(writer, leftPrec, rightPrec, prepareSqlNodes(call));
      } else if (call.operand(0).toString().contains(".")) {
        String firstOperand =
            removeSignFromLastOfStringAndAddInBeginning(call,
                call.operand(0).toString().replaceAll("[',]", ""));
        int scale = firstOperand.split("\\.")[1].length();
        parseToNumber(writer, leftPrec, rightPrec,
            new SqlNode[]{
                SqlLiteral.createCharString(firstOperand.trim(), SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("38", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric(String.valueOf(scale), SqlParserPos.ZERO)});
      }
      break;
    default:
      throw new IllegalArgumentException("Unsupported number of operands: "
          + call.getOperandList().size());
    }
  }

  public static void unparseToNumbertoConv(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect dialect) {
    SqlCall convCall =
        new SqlBasicCall(SqlLibraryOperators.CONV,
            new SqlNode[]{
                call.getOperandList().get(0),
                SqlLiteral.createExactNumeric("16", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    call.setOperand(0, convCall);
    handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.BIGINT, dialect);
  }

  public static boolean needsCustomUnparsing(SqlCall call) {
    if (((call.getOperandList().size() == 1 || call.getOperandList().size() == 3)
            && isOperandLiteral(call))
        || (call.getOperandList().size() == 2 && isOperandLiteral(call)
            && (isFirstOperandCurrencyType(call)
            || isOperandNull(call)
            || isOperandTypeOfCurrencyOrContainSpace(call)
            || call.operand(0).toString().contains(".")))) {
      return true;
    }
    return false;
  }

  /** Resolves the CAST target type for a TO_NUMBER call. */
  @FunctionalInterface
  private interface CastSpecResolver {
    SqlNode resolve(SqlCall call, SqlDialect dialect);
  }

  private static void unparseToNumberAsCast(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect dialect,
      CastSpecResolver castSpecResolver) {
    if (isOperandLiteral(call) && isOperandNull(call)) {
      handleNullOperand(writer, leftPrec, rightPrec, dialect);
      return;
    }

    switch (call.getOperandList().size()) {
    case 1:
    case 3:
      cleanCharStringLiteralOperand(call);
      castOperand(writer, call, leftPrec, rightPrec, dialect, castSpecResolver);
      break;
    case 2:
      if (isHexFormat(call)) {
        prependHexPrefix(call);
        handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.BIGINT, dialect);
      } else {
        if (!(call.operand(0) instanceof SqlIdentifier)) {
          modifyOperand(call);
        }
        castOperand(writer, call, leftPrec, rightPrec, dialect, castSpecResolver);
      }
      break;
    default:
      throw new IllegalArgumentException("Unsupported number of operands: "
          + call.getOperandList().size());
    }
  }

  private static void castOperand(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlDialect dialect,
      CastSpecResolver castSpecResolver) {
    SqlNode castSpec = castSpecResolver.resolve(call, dialect);
    handleCastingWithSpec(writer, call, leftPrec, rightPrec, castSpec);
  }

  private static void cleanCharStringLiteralOperand(SqlCall call) {
    if (call.operand(0) instanceof SqlCharStringLiteral) {
      String strippedValue = call.operand(0).toString().replaceAll(REGEX_REMOVE, "");
      call.setOperand(0,
          SqlLiteral.createCharString(strippedValue.trim(), SqlParserPos.ZERO));
    }
  }

  private static boolean isHexFormat(SqlCall call) {
    return HEX_FORMAT_PATTERN.matcher(call.operand(1).toString()).matches();
  }

  private static void prependHexPrefix(SqlCall call) {
    SqlCall concatCall =
        new SqlBasicCall(SqlStdOperatorTable.CONCAT,
            new SqlNode[]{
                SqlLiteral.createCharString("0x", SqlParserPos.ZERO),
                call.operand(0)},
            SqlParserPos.ZERO);
    call.setOperand(0, concatCall);
  }

  private static SqlNode resolveDefaultCastSpec(SqlCall call, SqlDialect dialect) {
    return castSpecForType(dialect, resolveDefaultSqlTypeName(call));
  }

  private static SqlTypeName resolveDefaultSqlTypeName(SqlCall call) {
    String operandText = call.operand(0).toString();
    if (operandText.contains(".")) {
      return SqlTypeName.FLOAT;
    }
    if (call.getOperandList().size() == 2
        && operandText.contains("E")
        && call.operand(1).toString().contains("E")) {
      return SqlTypeName.DECIMAL;
    }
    return SqlTypeName.BIGINT;
  }

  private static SqlNode resolveBigQueryCastSpec(SqlCall call, SqlDialect dialect) {
    if (!(call.operand(0) instanceof SqlCharStringLiteral)) {
      return castSpecForType(dialect, SqlTypeName.BIGINT);
    }
    String value = ((SqlCharStringLiteral) call.operand(0)).getValueAs(String.class);
    return resolveBigQueryCastSpecFromValue(value, dialect);
  }

  /**
   * Resolves the CAST target type spec that {@link #unparseToNumberBigQuery} would use for a
   * {@code TO_NUMBER} call. Used to detect redundant {@code CAST(TO_NUMBER(...) AS T)} wrappers.
   */
  public static SqlNode resolveBigQueryCastSpecForCall(SqlCall call, SqlDialect dialect) {
    return resolveBigQueryCastSpec(call, dialect);
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

  private static SqlNode castSpecForType(SqlDialect dialect, SqlTypeName typeName) {
    return Objects.requireNonNull(
        dialect.getCastSpec(
        new BasicSqlType(RelDataTypeSystem.DEFAULT, typeName)));
  }

  private static void handleCasting(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
      SqlTypeName sqlTypeName, SqlDialect dialect) {
    handleCastingWithSpec(writer, call, leftPrec, rightPrec, castSpecForType(dialect, sqlTypeName));
  }

  private static void handleCastingWithSpec(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlNode castSpec) {
    SqlCall castCall =
        new SqlBasicCall(SqlStdOperatorTable.CAST,
            new SqlNode[]{call.operand(0), castSpec},
            SqlParserPos.ZERO);
    writer.getDialect().unparseCall(writer, castCall, leftPrec, rightPrec);
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

  private static void modifyOperand(SqlCall call) {
    String regEx = call.operand(1).toString().contains("C") ? REGEX_REMOVE : "[',$]+";
    String firstOperand =
        removeSignFromLastOfStringAndAddInBeginning(call,
            call.operand(0).toString().replaceAll(regEx, ""));
    call.setOperand(0,
        SqlLiteral.createCharString(firstOperand.trim(), SqlParserPos.ZERO));
  }

  private static String removeSignFromLastOfStringAndAddInBeginning(SqlCall call,
      String firstOperand) {
    if (call.operand(1).toString().contains("MI") || call.operand(1).toString().contains("S")) {
      if (call.operand(0).toString().contains("-")) {
        firstOperand = firstOperand.replaceAll("-", "");
        firstOperand = "-" + firstOperand;
      } else {
        firstOperand = firstOperand.replaceAll("\\+", "");
      }
    }
    return firstOperand;
  }

  private static void handleNullOperand(
      SqlWriter writer, int leftPrec, int rightPrec, SqlDialect dialect) {
    SqlCall castCall =
        new SqlBasicCall(SqlStdOperatorTable.CAST,
            new SqlNode[]{
                new SqlDataTypeSpec(
                    new SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
                    SqlParserPos.ZERO),
                castSpecForType(dialect, SqlTypeName.INTEGER)},
            SqlParserPos.ZERO);
    writer.getDialect().unparseCall(writer, castCall, leftPrec, rightPrec);
  }

  private static boolean isOperandNull(SqlCall call) {
    for (SqlNode sqlNode : call.getOperandList()) {
      SqlLiteral literal = (SqlLiteral) sqlNode;
      if (literal.getValue() == null) {
        return true;
      }
    }
    return false;
  }

  private static boolean isOperandLiteral(SqlCall call) {
    return call.operand(0) instanceof SqlCharStringLiteral
        || call.operand(0) instanceof SqlLiteral;
  }

  private static boolean isFirstOperandCurrencyType(SqlCall call) {
    return call.operand(0).toString().contains("$") && (call.operand(1).toString().contains("L")
        || call.operand(1).toString().contains("U"));
  }

  private static boolean isOperandTypeOfCurrencyOrContainSpace(SqlCall call) {
    return call.operand(1).toString().contains("PR")
        || (call.operand(0).toString().contains("USD")
        && call.operand(1).toString().contains("C"));
  }

  private static SqlNode[] prepareSqlNodes(SqlCall call) {
    if (isOperandNull(call)) {
      return new SqlNode[]{new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
          SqlParserPos.ZERO)};
    }
    String firstOperand = call.operand(0).toString().replaceAll(REGEX_REMOVE, "");
    if (firstOperand.contains(".")) {
      int scale = firstOperand.split("\\.")[1].length();
      return new SqlNode[]{
          SqlLiteral.createCharString(firstOperand.trim(), SqlParserPos.ZERO),
          SqlLiteral.createExactNumeric("38", SqlParserPos.ZERO),
          SqlLiteral.createExactNumeric(String.valueOf(scale), SqlParserPos.ZERO)};
    }
    return new SqlNode[]{
        SqlLiteral.createCharString(firstOperand.trim(), SqlParserPos.ZERO)};
  }

  private static void parseToNumber(SqlWriter writer, int leftPrec, int rightPrec,
      SqlNode[] operands) {
    SqlCall toNumberCall =
        new SqlBasicCall(SqlStdOperatorTable.TO_NUMBER, operands, SqlParserPos.ZERO);
    SqlStdOperatorTable.TO_NUMBER.unparse(writer, toNumberCall, leftPrec, rightPrec);
  }
}

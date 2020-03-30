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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.FormatFunctionUtil;
import org.apache.calcite.util.ToNumberUtils;

/**
 * A <code>SqlDialect</code> implementation for the Snowflake database.
 */
public class SnowflakeSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new SnowflakeSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.SNOWFLAKE)
          .withIdentifierQuoteString("\"")
          .withUnquotedCasing(Casing.TO_UPPER)
          .withConformance(SqlConformanceEnum.SNOWFLAKE));

  /** Creates a SnowflakeSqlDialect. */
  public SnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public SqlOperator getTargetFunc(RexCall call) {
    switch (call.type.getSqlTypeName()) {
    case DATE:
    case TIMESTAMP:
      return getTargetFunctionForDateOperations(call);
    default:
      return super.getTargetFunc(call);
    }
  }

  private SqlOperator getTargetFunctionForDateOperations(RexCall call) {
    switch (call.getOperands().get(1).getType().getSqlTypeName()) {
    case INTERVAL_DAY:
    case INTERVAL_MONTH:
      if (call.op.kind == SqlKind.MINUS) {
        return SqlLibraryOperators.DATE_SUB;
      }
      return SqlLibraryOperators.DATE_ADD;
    }
    return super.getTargetFunc(call);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final
  int leftPrec, final int rightPrec) {
    switch (call.getKind()) {
    case SUBSTRING:
      final SqlWriter.Frame substringFrame = writer.startFunCall("SUBSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(substringFrame);
      break;
    case TO_NUMBER:
      if (ToNumberUtils.needsCustomUnparsing(call)) {
        ToNumberUtils.unparseToNumberSnowFlake(writer, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case CHAR_LENGTH:
      final SqlWriter.Frame lengthFrame = writer.startFunCall("LENGTH");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(lengthFrame);
      break;
    case FORMAT:
      FormatFunctionUtil ffu = new FormatFunctionUtil();
      SqlCall sqlCall = ffu.fetchSqlCallForFormat(call);
      super.unparseCall(writer, sqlCall, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * For usage of DATE_ADD,DATE_SUB function in Snowflake. It will unparse the SqlCall and write it
   * into Snowflake format. Below are few examples:
   * Example 1:
   * Input: select date + INTERVAL 1 DAY
   * It will write output query as: select (date + 1)
   * Example 2:
   * Input: select date + Store_id * INTERVAL 2 DAY
   * It will write output query as: select (date + Store_id * 2)
   *
   * @param writer Target SqlWriter to write the call
   * @param call SqlCall : date + Store_id * INTERVAL 2 DAY
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate left precision
   */
  @Override public void unparseIntervalOperandsBasedFunctions(
    SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {

    switch (call.operand(1).getKind()) {
    case LITERAL:
      unparseSqlIntervalLiteral(writer, call, leftPrec, rightPrec);
      break;
    case TIMES:
      unparseExpressionIntervalCall(writer, call, leftPrec, rightPrec);
      break;
    default:
      throw new AssertionError(call.operand(1).getKind() + " is not valid");
    }

  }

  /**
   * Unparse the literal call from input query and write the INTERVAL part. Below is an example:
   * Input: INTERVAL 2 DAY
   * It will write this as: 2
   *
   //* @param literal SqlIntervalLiteral :INTERVAL 1 DAY
   * @param writer Target SqlWriter to write the call
   */
  public void unparseSqlIntervalLiteral(
        SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame;
    SqlIntervalLiteral literal = call.operand(1);
    SqlTypeName type = literal.getTypeName();
    SqlIntervalLiteral.IntervalValue interval =
          (SqlIntervalLiteral.IntervalValue) literal.getValue();
    switch (type) {
    case INTERVAL_DAY:
      frame = writer.startList("(", ")");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep((SqlKind.PLUS == call.getKind()) ? "+" : "-");
      if (interval.getSign() == -1) {
        writer.print("(-");
        writer.literal(interval.getIntervalLiteral());
        writer.print(")");
      } else {
        writer.literal(interval.getIntervalLiteral());
      }
      writer.endList(frame);
      break;

    case INTERVAL_MONTH:
      frame = writer.startFunCall("DATEADD");
      handleIntervalLiteralForMonth(writer, call, leftPrec, rightPrec, frame, literal);
      break;
    default:
      throw new IllegalArgumentException(call.getOperator().toString() + " is not handled");
    }


  }

  private void handleIntervalLiteralForMonth(SqlWriter writer, SqlCall call,
                                             int leftPrec, int rightPrec,
                                             SqlWriter.Frame frame,
                                             SqlIntervalLiteral intervalLiteral) {
    int intervalValue = getIntervalValue(intervalLiteral, call);
    SqlIntervalLiteral.IntervalValue interval =
          (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
    SqlIntervalQualifier qualifier = interval.getIntervalQualifier();
    writer.print(qualifier.toString());
    writer.print(", ");
    writer.print(intervalValue);
    writer.print(",");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(frame);
  }

  private int getIntervalValue(SqlIntervalLiteral intervalLiteral, SqlCall call) {
    SqlIntervalLiteral.IntervalValue literalValue =
          (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
    int intLiteral = Integer.parseInt(literalValue.getIntervalLiteral());
    int sign = literalValue.getSign();
    if (SqlKind.MINUS == call.getKind()) {
      return intLiteral * sign * -1;
    }
    return intLiteral * sign;
  }

  /**
   * Unparse the SqlBasic call and write INTERVAL with expression. Below are the examples:
   * Example 1:
   * Input: store_id * INTERVAL 1 DAY
   * It will write this as: store_id
   * Example 2:
   * Input: 10 * INTERVAL 2 DAY
   * It will write this as: 10 * 2
   *
   //* @param call SqlCall : store_id * INTERVAL 1 DAY
   * @param writer Target SqlWriter to write the call
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate right precision
   */
  private void unparseExpressionIntervalCall(
        SqlWriter writer, SqlCall sqlCall, int leftPrec, int rightPrec) {
    SqlBasicCall secondOperand = sqlCall.operand(1);
    SqlTypeName type = ((SqlIntervalLiteral) (secondOperand.operand(1))).getTypeName();
    final SqlWriter.Frame frame;
    SqlLiteral intervalLiteral = getIntervalLiteral(secondOperand);
    SqlIntervalLiteral.IntervalValue literalValue =
          (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
    SqlNode identifier = getIdentifier(secondOperand);
    switch (type) {
    case INTERVAL_DAY:
      frame = writer.startList("(", ")");
      sqlCall.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep((SqlKind.PLUS == sqlCall.getKind()) ? "+" : "-");
      if (secondOperand.getKind() == SqlKind.TIMES) {
        identifier.unparse(writer, leftPrec, rightPrec);
        if (!literalValue.getIntervalLiteral().equals("1")) {
          writer.sep("*");
          writer.sep(literalValue.toString());
        }
      }
      writer.endFunCall(frame);
      break;

    case INTERVAL_MONTH:
      switch (sqlCall.getOperator().toString()) {
      case "DATE_ADD":
      case "DATE_SUB":
        frame = writer.startFunCall("DATEADD");
        SqlIntervalQualifier qualifier = literalValue.getIntervalQualifier();
        writer.print(qualifier.toString());
        writer.print(",");
        identifier.unparse(writer, leftPrec, rightPrec);
        if (!literalValue.getIntervalLiteral().equals("1")) {
          writer.sep("*");
          writer.sep(literalValue.toString());
        }
        writer.print(",");
        sqlCall.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
        break;

      default:
        throw new IllegalArgumentException(
            secondOperand.getOperator().toString() + " is not handled for Snowflake");
      }
      break;

    }


  }

  /**
   * Return the SqlLiteral from the SqlBasicCall.
   *
   * @param intervalOperand store_id * INTERVAL 1 DAY
   * @return SqlLiteral INTERVAL 1 DAY
   */
  private SqlLiteral getIntervalLiteral(SqlBasicCall intervalOperand) {
    if (intervalOperand.operand(1).getKind() == SqlKind.IDENTIFIER
        || (intervalOperand.operand(1) instanceof SqlNumericLiteral)) {
      return ((SqlBasicCall) intervalOperand).operand(0);
    }
    return ((SqlBasicCall) intervalOperand).operand(1);
  }

  /**
   * Return the identifer from the SqlBasicCall.
   *
   * @param intervalOperand Store_id * INTERVAL 1 DAY
   * @return SqlIdentifier Store_id
   */
  private SqlNode getIdentifier(SqlBasicCall intervalOperand) {
    if (intervalOperand.operand(1).getKind() == SqlKind.IDENTIFIER
        || (intervalOperand.operand(1) instanceof SqlNumericLiteral)) {
      return intervalOperand.operand(1);
    }
    return intervalOperand.operand(0);
  }

}

// End SnowflakeSqlDialect.java

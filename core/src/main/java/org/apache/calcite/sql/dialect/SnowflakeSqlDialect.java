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
import org.apache.calcite.util.ToNumberUtils;

/**
 * A <code>SqlDialect</code> implementation for the Snowflake database.
 */
public class SnowflakeSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new SnowflakeSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.SNOWFLAKE)
          .withIdentifierQuoteString("\"")
          .withUnquotedCasing(Casing.TO_UPPER));

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
    case TIMESTAMP:
    case DATE:
      return getTargetFunctionForDateTSOperations(call);
    default:
      return super.getTargetFunc(call);
    }
  }

  private SqlOperator getTargetFunctionForDateTSOperations(RexCall call) {
    switch (call.type.getSqlTypeName()) {
    case DATE:
    case TIMESTAMP:
      switch (call.getOperands().get(1).getType().getSqlTypeName()) {
      case INTERVAL_DAY:
      case INTERVAL_MONTH:
        return SqlLibraryOperators.DATE_ADD;
      }
    default:
      return super.getTargetFunc(call);
    }
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
   * @param writer    Target SqlWriter to write the call
   * @param call      SqlCall : date + Store_id * INTERVAL 2 DAY
   * @param leftPrec  Indicate left precision
   * @param rightPrec Indicate left precision
   */
  @Override public void unparseIntervalOperandsBasedFunctions(
    SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {

    switch (call.operand(1).getKind()) {
    case LITERAL:
      unparseIntervalLiteral(writer, call, leftPrec, rightPrec);
      break;
    case TIMES:
      unparseExpressionIntervalCall(writer, call, leftPrec, rightPrec);
      break;

    default:
      throw new AssertionError(call.operand(1).getKind() + " is not valid");
    }
  }

  private void unparseIntervalLiteral(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlWriter.Frame frame;
    switch (call.getOperator().toString()) {
    case "DATE_ADD":
      frame = writer.startFunCall("DATEADD");
      SqlIntervalLiteral intervalLiteral = call.operand(1);
      handleIntervalLiteral(writer, call, leftPrec, rightPrec, frame, intervalLiteral);
      break;
    default:
      throw new IllegalArgumentException(call.getOperator().toString() + " is not handled");
    }
  }

  private void handleIntervalLiteral(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
                                     SqlWriter.Frame frame, SqlIntervalLiteral intervalLiteral) {
    switch (intervalLiteral.getKind()) {
    case LITERAL:
      SqlTypeName typeName = intervalLiteral.getTypeName();
      switch (typeName) {
      case INTERVAL_MONTH:
        writer.print("MONTH");
        break;
      case INTERVAL_DAY:
        writer.print("DAY");
        break;
      default:
        throw new AssertionError(typeName + " is not handled/invalid");
      }
      break;
    default:
      throw new AssertionError(intervalLiteral.getKind() + " is not valid for this method");
    }
    writer.print(", ");
    SqlIntervalLiteral.IntervalValue literalValue =
          (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
    int intLiteral = Integer.parseInt(literalValue.getIntervalLiteral());
    int sign = literalValue.getSign();
    int intervalValue = intLiteral * sign;
    writer.print(intervalValue);
    writer.print(",");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(frame);
  }


  /**
   * Unparse the literal call from input query and write the INTERVAL part. Below is an example:
   * Input: INTERVAL 2 DAY
   * It will write this as: 2
   *
   * @param literal SqlIntervalLiteral :INTERVAL 1 DAY
   * @param writer  Target SqlWriter to write the call
   */
  @Override public void unparseSqlIntervalLiteral(
      SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    SqlIntervalLiteral.IntervalValue interval =
                      (SqlIntervalLiteral.IntervalValue) literal.getValue();
    if (interval.getSign() == -1) {
      writer.print("(-");
      writer.literal(interval.getIntervalLiteral());
      writer.print(")");
    } else {
      writer.literal(interval.getIntervalLiteral());
    }
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
   * @param call      SqlCall : store_id * INTERVAL 1 DAY
   * @param writer    Target SqlWriter to write the call
   */
  private void unparseExpressionIntervalCall(
        SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlWriter.Frame frame;

    switch (call.getOperator().toString()) {
    case "DATE_ADD":
      frame = writer.startFunCall("DATEADD");

      SqlLiteral intervalLiteral = getIntervalLiteral(call.operand(1));
      SqlNode identifier = getIdentifier(call.operand(1));
      SqlIntervalLiteral.IntervalValue literalValue =
              (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
      SqlIntervalQualifier qualifier = literalValue.getIntervalQualifier();


      writer.print(qualifier.toString());
      writer.print(",");
      identifier.unparse(writer, leftPrec, rightPrec);

      if (!literalValue.getIntervalLiteral().equals("1")) {
        writer.sep("*");
        writer.sep(literalValue.toString());
      }
      writer.print(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(frame);

      break;

    default:
      throw new IllegalArgumentException(
        call.getOperator().toString() + " is not handled for Snowflake");
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

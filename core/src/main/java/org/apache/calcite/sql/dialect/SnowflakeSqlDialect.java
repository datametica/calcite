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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.FormatFunctionUtil;
import org.apache.calcite.util.ToNumberUtils;
import org.apache.calcite.util.interval.SnowflakeDateTimestampInterval;

import org.apache.commons.lang3.StringUtils;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_DATE;

/**
 * A <code>SqlDialect</code> implementation for the Snowflake database.
 */
public class SnowflakeSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.SNOWFLAKE)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.TO_UPPER);

  public static final SqlDialect DEFAULT =
      new SnowflakeSqlDialect(DEFAULT_CONTEXT);

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
      return getTargetFunctionForDateOperations(call);
    default:
      return super.getTargetFunc(call);
    }
  }

  private SqlOperator getTargetFunctionForDateOperations(RexCall call) {
    switch (call.getOperands().get(1).getType().getSqlTypeName()) {
    case INTERVAL_DAY:
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
    case CHAR_LENGTH:
      SqlCall lengthCall = SqlLibraryOperators.LENGTH
              .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
      break;
    case ENDS_WITH:
      SqlCall endsWithCall = SqlLibraryOperators.ENDSWITH
              .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, endsWithCall, leftPrec, rightPrec);
      break;
    case STARTS_WITH:
      SqlCall startsWithCall = SqlLibraryOperators.STARTSWITH
              .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, startsWithCall, leftPrec, rightPrec);
      break;
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
    case FORMAT:
      FormatFunctionUtil ffu = new FormatFunctionUtil();
      SqlCall sqlCall = ffu.fetchSqlCallForFormat(call);
      super.unparseCall(writer, sqlCall, leftPrec, rightPrec);
      break;
    case TRIM:
      unparseTrim(writer, call, leftPrec, rightPrec);
      break;
    case TRUNCATE:
    case IF:
    case OTHER_FUNCTION:
    case OTHER:
      unparseOtherFunction(writer, call, leftPrec, rightPrec);
      break;
    case TIMESTAMP_DIFF:
      final SqlWriter.Frame timestampdiff = writer.startFunCall("TIMESTAMPDIFF");
      call.operand(2).unparse(writer, leftPrec, rightPrec);
      writer.print(", ");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.print(", ");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(timestampdiff);
      break;
    case DIVIDE_INTEGER:
      unparseDivideInteger(writer, call, leftPrec, rightPrec);
      break;
    case POSITION:
      unparsePosition(writer, call, leftPrec, rightPrec);
      break;
    case OVER:
      handleOverCall(writer, call, leftPrec, rightPrec);
      break;
    case TIMES:
      unparseIntervalTimes(writer, call, leftPrec, rightPrec);
      break;
    case PLUS:
      SnowflakeDateTimestampInterval interval = new SnowflakeDateTimestampInterval();
      if (!interval.handlePlus(writer, call, leftPrec, rightPrec)) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case MINUS:
      SnowflakeDateTimestampInterval interval1 = new SnowflakeDateTimestampInterval();
      if (!interval1.handleMinus(writer, call, leftPrec, rightPrec, "-")) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseIntervalTimes(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.operand(0) instanceof SqlIntervalLiteral) {
      SqlCall multipleCall = new SnowflakeDateTimestampInterval().unparseMultipleInterval(call);
      multipleCall.unparse(writer, leftPrec, rightPrec);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void handleOverCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (checkWindowFunctionContainOrderBy(call)) {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    } else {
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      unparseSqlWindow(call.operand(1), writer, call);
    }
  }

  private boolean checkWindowFunctionContainOrderBy(SqlCall call) {
    return !((SqlWindow) call.operand(1)).getOrderList().getList().isEmpty();
  }

  private void unparseSqlWindow(SqlWindow sqlWindow, SqlWriter writer, SqlCall call) {
    final SqlWindow window = sqlWindow;
    writer.print("OVER ");
    SqlCall operand1 = call.operand(0);
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.WINDOW, "(", ")");
    if (window.getRefName() != null) {
      window.getRefName().unparse(writer, 0, 0);
    }
    if (window.getOrderList().size() == 0) {
      if (window.getPartitionList().size() > 0) {
        writer.sep("PARTITION BY");
        final SqlWriter.Frame partitionFrame = writer.startList("", "");
        window.getPartitionList().unparse(writer, 0, 0);
        writer.endList(partitionFrame);
      }
      writer.print("ORDER BY ");
      if (operand1.getOperandList().size() == 0) {
        writer.print("0 ");
      } else {
        SqlNode operand2 = operand1.operand(0);
        operand2.unparse(writer, 0, 0);
      }
      writer.print("ROWS BETWEEN ");
      writer.sep(window.getLowerBound().toString());
      writer.sep("AND");
      writer.sep(window.getUpperBound().toString());
    }
    writer.endList(frame);
  }

  private void unparsePosition(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame regexp_instr = writer.startFunCall("REGEXP_INSTR");
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(regexp_instr);
  }

  private void unparseOtherFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperator().getName()) {
    case "TRUNCATE":
      handleMathFunction(writer, call, leftPrec, rightPrec);
      break;
    case "ROUND":
      unparseRoundfunction(writer, call, leftPrec, rightPrec);
      break;
    case "TIME_DIFF":
      unparseTimeDiff(writer, call, leftPrec, rightPrec);
      break;
    case "TIMESTAMPINTADD":
    case "TIMESTAMPINTSUB":
      SqlWriter.Frame timestampAdd = writer.startFunCall(fetchFunctionName(call));
      writer.print("SECOND, ");
      call.getOperandList().get(call.getOperandList().size() - 1)
              .unparse(writer, leftPrec, rightPrec);
      writer.print(", ");
      call.getOperandList().get(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(timestampAdd);
      break;
    case "FORMAT_DATE":
      final SqlWriter.Frame formatDate = writer.startFunCall("TO_VARCHAR");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.print(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(formatDate);
      break;
    case "LOG10":
      if (call.operand(0) instanceof SqlLiteral && "1".equals(call.operand(0).toString())) {
        writer.print(0);
      } else {
        final SqlWriter.Frame logFrame = writer.startFunCall("LOG");
        writer.print("10");
        writer.print(", ");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(logFrame);
      }
      break;
    case "IF":
      unparseIf(writer, call, leftPrec, rightPrec);
      break;
    case "STR_TO_DATE":
      SqlCall parseDateCall =
          TO_DATE.createCall(SqlParserPos.ZERO, call.operand(0), call.operand(1));
      unparseCall(writer, parseDateCall, leftPrec, rightPrec);
      break;
    case "INSTR":
      final SqlWriter.Frame regexpInstr = writer.startFunCall("REGEXP_INSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(regexpInstr);
      break;
    case "DATE_MOD":
      unparseDateModule(writer, call, leftPrec, rightPrec);
      break;
    case "RAND_INTEGER":
      unparseRandom(writer, call, leftPrec, rightPrec);
      break;
    case "DATE_DIFF":
      unparseDateDiff(writer, call, leftPrec, rightPrec);
      break;
    case DateTimestampFormatUtil.WEEKNUMBER_OF_YEAR:
    case DateTimestampFormatUtil.YEARNUMBER_OF_CALENDAR:
    case DateTimestampFormatUtil.MONTHNUMBER_OF_YEAR:
    case DateTimestampFormatUtil.QUARTERNUMBER_OF_YEAR:
    case DateTimestampFormatUtil.MONTHNUMBER_OF_QUARTER:
    case DateTimestampFormatUtil.WEEKNUMBER_OF_MONTH:
    case DateTimestampFormatUtil.WEEKNUMBER_OF_CALENDAR:
    case DateTimestampFormatUtil.DAYOCCURRENCE_OF_MONTH:
    case DateTimestampFormatUtil.DAYNUMBER_OF_CALENDAR:
      DateTimestampFormatUtil dateTimestampFormatUtil = new DateTimestampFormatUtil();
      dateTimestampFormatUtil.unparseCall(writer, call, leftPrec, rightPrec);
      break;
    case "TIMESTAMP_SECONDS":
      final SqlWriter.Frame timestampSecond = writer.startFunCall("TO_TIMESTAMP");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(timestampSecond);
      break;
    case "TO_CHAR":
      if (call.operand(1) instanceof SqlLiteral) {
        String val = ((SqlLiteral) call.operand(1)).getValueAs(String.class);
        if (val.equalsIgnoreCase("day")) {
          unparseToChar(writer, call, leftPrec, rightPrec, val);
          break;
        }
      }
      super.unparseCall(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private String fetchFunctionName(SqlCall call) {
    String operatorName = call.getOperator().getName();
    return operatorName.equals("TIMESTAMPINTADD") ? "DATEADD"
            : operatorName.equals("TIMESTAMPINTSUB") ? "DATEDIFF" : operatorName;
  }

  private String getDay(String day, String caseType) {
    if (caseType.equals("DAY")) {
      return StringUtils.upperCase(day);
    } else if (caseType.equals("Day")) {
      return day;
    } else {
      return StringUtils.lowerCase(day);
    }
  }

  private void unparseToChar(SqlWriter writer, SqlCall call, int leftPrec,
                             int rightPrec, String day) {
    writer.print("CASE ");
    SqlWriter.Frame dayNameFrame = writer.startFunCall("DAYNAME");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(dayNameFrame);
    writer.print("WHEN 'Sun' THEN ");
    writer.print(getDay("'Sunday ' ", day));
    writer.print("WHEN 'Mon' THEN ");
    writer.print(getDay("'Monday ' ", day));
    writer.print("WHEN 'Tue' THEN ");
    writer.print(getDay("'Tuesday ' ", day));
    writer.print("WHEN 'Wed' THEN ");
    writer.print(getDay("'Wednesday ' ", day));
    writer.print("WHEN 'Thu' THEN ");
    writer.print(getDay("'Thursday ' ", day));
    writer.print("WHEN 'Fri' THEN ");
    writer.print(getDay("'Friday ' ", day));
    writer.print("WHEN 'Sat' THEN ");
    writer.print(getDay("'Saturday ' ", day));
    writer.print("END");
  }

  private void unparseTimeDiff(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame timeDiff = writer.startFunCall("TIMEDIFF");
    writer.sep(",");
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(timeDiff);
  }

  private void unparseDateDiff(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame dateDiffFrame = writer.startFunCall("DATEDIFF");
    int size = call.getOperandList().size();
    for (int index = size - 1; index >= 0; index--) {
      writer.sep(",");
      call.operand(index).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(dateDiffFrame);
  }

  /**
   * unparse method for round function.
   */
  private void unparseRoundfunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame castFrame = writer.startFunCall("TO_DECIMAL");
    handleMathFunction(writer, call, leftPrec, rightPrec);
    writer.print(",38, 4");
    writer.endFunCall(castFrame);
  }

  /**
   * unparse method for random function
   * within the range of specific values.
   */
  private void unparseRandom(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame randFrame = writer.startFunCall("UNIFORM");
    writer.sep(",");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    writer.print("RANDOM()");
    writer.endFunCall(randFrame);
  }

  /**
   * unparse function for math functions
   * SF can support precision and scale within specific range
   * handled precision range using 'case', 'when', 'then'.
   */
  private void handleMathFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame mathFun = writer.startFunCall(call.getOperator().getName());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    if (call.getOperandList().size() > 1) {
      writer.print(",");
      if (call.operand(1) instanceof SqlNumericLiteral) {
        call.operand(1).unparse(writer, leftPrec, rightPrec);
      } else {
        writer.print("CASE WHEN ");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.print("> 38 THEN 38 ");
        writer.print("WHEN ");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.print("< -12 THEN -12 ");
        writer.print("ELSE ");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.print("END");
      }
    }
    writer.endFunCall(mathFun);
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
    final SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep((SqlKind.PLUS == call.getKind()) ? "+" : "-");
    switch (call.operand(1).getKind()) {
    case LITERAL:
      unparseSqlIntervalLiteral(writer, call.operand(1), leftPrec, rightPrec);
      break;
    case TIMES:
      unparseExpressionIntervalCall(writer, call.operand(1), leftPrec, rightPrec);
      break;
    default:
      throw new AssertionError(call.operand(1).getKind() + " is not valid");
    }
    writer.endList(frame);
  }

  /**
   * Unparse the literal call from input query and write the INTERVAL part. Below is an example:
   * Input: INTERVAL 2 DAY
   * It will write this as: 2
   *
   * @param literal SqlIntervalLiteral :INTERVAL 1 DAY
   * @param writer Target SqlWriter to write the call
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
   * @param call SqlCall : store_id * INTERVAL 1 DAY
   * @param writer Target SqlWriter to write the call
   * @param leftPrec Indicate left precision
   * @param rightPrec Indicate right precision
   */
  private void unparseExpressionIntervalCall(
      SqlWriter writer, SqlBasicCall call, int leftPrec, int rightPrec) {
    SqlLiteral intervalLiteral = getIntervalLiteral(call);
    SqlNode identifier = getIdentifier(call);
    SqlIntervalLiteral.IntervalValue literalValue =
        (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
    if (call.getKind() == SqlKind.TIMES) {
      identifier.unparse(writer, leftPrec, rightPrec);
      if (!literalValue.getIntervalLiteral().equals("1")) {
        writer.sep("*");
        writer.sep(literalValue.toString());
      }
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
     * For usage of TRIM, LTRIM and RTRIM in SnowFlake.
     */
  private void unparseTrim(
      SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    assert call.operand(0) instanceof SqlLiteral : call.operand(0);
    final String operatorName;
    SqlLiteral trimFlag = call.operand(0);
    SqlLiteral valueToTrim = call.operand(1);
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      operatorName = "LTRIM";
      break;
    case TRAILING:
      operatorName = "RTRIM";
      break;
    default:
      operatorName = call.getOperator().getName();
    }
    final SqlWriter.Frame trimFrame = writer.startFunCall(operatorName);
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    if (!valueToTrim.toValue().matches("\\s+")) {
      writer.literal(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(trimFrame);
  }

  /**
   * For usage of IFF() in snowflake.
   */
  private void unparseIf(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame iffFrame = writer.startFunCall("IFF");
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(iffFrame);
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


  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall, RelDataType relDataType) {
    return ((SqlBasicCall) aggCall).operand(0);
  }

}

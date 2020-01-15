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

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.CurrentTimestampUtils;
import org.apache.calcite.util.ToNumberUtils;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IF;

/**
 * A <code>SqlDialect</code> implementation for the Apache Hive database.
 */
public class HiveSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new HiveSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.HIVE)
          .withNullCollation(NullCollation.LOW)
          .withConformance(SqlConformanceEnum.HIVE));

  private final boolean emulateNullDirection;

  /** Creates a HiveSqlDialect. */
  public HiveSqlDialect(Context context) {
    super(context);
    // Since 2.1.0, Hive natively supports "NULLS FIRST" and "NULLS LAST".
    // See https://issues.apache.org/jira/browse/HIVE-12994.
    emulateNullDirection = (context.databaseMajorVersion() < 2)
        || (context.databaseMajorVersion() == 2
            && context.databaseMinorVersion() < 1);
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsColumnAliasInSort() {
    return true;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public boolean supportsGroupByWithRollup() {
    return true;
  }

  @Override public boolean supportsAnalyticalFunctionInAggregate() {
    return false;
  }

  @Override public boolean supportsAnalyticalFunctionInGroupBy() {
    return false;
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    if (emulateNullDirection) {
      return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    }

    return null;
  }

  @Override public SqlOperator getTargetFunc(RexCall call) {
    switch (call.type.getSqlTypeName()) {
    case DATE:
      switch (call.getOperands().get(1).getType().getSqlTypeName()) {
      case INTERVAL_DAY:
        return SqlLibraryOperators.DATE_ADD;
      case INTERVAL_MONTH:
        return SqlLibraryOperators.ADD_MONTHS;
      }
    default:
      return super.getTargetFunc(call);
    }
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call,
      final int leftPrec, final int rightPrec) {
    switch (call.getKind()) {
    case POSITION:
      final SqlWriter.Frame frame = writer.startFunCall("INSTR");
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      if (3 == call.operandCount()) {
        throw new RuntimeException("3rd operand Not Supported for Function INSTR in Hive");
      }
      writer.endFunCall(frame);
      break;
    case MOD:
      SqlOperator op = SqlStdOperatorTable.PERCENT_REMAINDER;
      SqlSyntax.BINARY.unparse(writer, op, call, leftPrec, rightPrec);
      break;
    case TRIM:
      unparseTrim(writer, call, leftPrec, rightPrec);
      break;
    case CHAR_LENGTH:
    case CHARACTER_LENGTH:
      final SqlWriter.Frame lengthFrame = writer.startFunCall("LENGTH");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(lengthFrame);
      break;
    case SUBSTRING:
      final SqlWriter.Frame substringFrame = writer.startFunCall("SUBSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(substringFrame);
      break;
    case EXTRACT:
      final SqlWriter.Frame extractFrame = writer.startFunCall(call.operand(0).toString());
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(extractFrame);
      break;
    case ARRAY_VALUE_CONSTRUCTOR:
      writer.keyword(call.getOperator().getName());
      final SqlWriter.Frame arrayFrame = writer.startList("(", ")");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(arrayFrame);
      break;
    case CONCAT:
      final SqlWriter.Frame concatFrame = writer.startFunCall("CONCAT");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(concatFrame);
      break;
    case DIVIDE_INTEGER:
      unparseDivideInteger(writer, call, leftPrec, rightPrec);
      break;
    case FORMAT:
      unparseFormat(writer, call, leftPrec, rightPrec);
      break;
    case TO_NUMBER:
      ToNumberUtils.handleToNumber(writer, call, leftPrec, rightPrec);
      break;
    case NULLIF:
      unparseNullIf(writer, call, leftPrec, rightPrec);
      break;
    case OTHER_FUNCTION:
      if (call.getOperator().getName().equals(CURRENT_TIMESTAMP.getName())) {
        CurrentTimestampUtils.unparseCurrentTimestamp(writer, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * For usage of TRIM, LTRIM and RTRIM in Hive, see
   * <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF">Hive UDF usage</a>.
   */
  private void unparseTrim(SqlWriter writer, SqlCall call, int leftPrec,
                           int rightPrec) {
    assert call.operand(0) instanceof SqlLiteral : call.operand(0);
    SqlLiteral trimFlag = call.operand(0);
    SqlLiteral valueToTrim = call.operand(1);
    if (valueToTrim.toValue().matches("\\s+")) {
      handleTrimWithSpace(writer, call, leftPrec, rightPrec, trimFlag);
    } else {
      handleTrimWithChar(writer, call, leftPrec, rightPrec, trimFlag);
    }
  }

  private void handleTrimWithSpace(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlLiteral trimFlag) {
    final String operatorName;
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      operatorName = "LTRIM";
      break;
    case TRAILING:
      operatorName = "RTRIM";
      break;
    default:
      operatorName = call.getOperator().getName();
      break;
    }
    final SqlWriter.Frame trimFrame = writer.startFunCall(operatorName);
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(trimFrame);
  }

  private void handleTrimWithChar(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, SqlLiteral trimFlag) {
    SqlCharStringLiteral regexNode = makeRegexNodeFromCall(call.operand(1), trimFlag);
    SqlCharStringLiteral blankLiteral = SqlLiteral.createCharString("",
        call.getParserPosition());
    SqlNode[] trimOperands = new SqlNode[]{call.operand(2), regexNode, blankLiteral};
    SqlCall regexReplaceCall = new SqlBasicCall(REGEXP_REPLACE, trimOperands, SqlParserPos.ZERO);
    REGEXP_REPLACE.unparse(writer, regexReplaceCall, leftPrec, rightPrec);
  }

  private SqlCharStringLiteral makeRegexNodeFromCall(SqlNode call, SqlLiteral trimFlag) {
    String regexPattern = ((SqlCharStringLiteral) call).toValue();
    regexPattern = escapeSpecialChar(regexPattern);
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      regexPattern = "^(".concat(regexPattern).concat(")*");
      break;
    case TRAILING:
      regexPattern = "(".concat(regexPattern).concat(")*$");
      break;
    default:
      regexPattern = "^(".concat(regexPattern).concat(")*|(")
          .concat(regexPattern).concat(")*$");
      break;
    }
    return SqlLiteral.createCharString(regexPattern,
        call.getParserPosition());
  }

  private String escapeSpecialChar(String inputString) {
    final String[] specialCharacters = {"\\", "^", "$", "{", "}", "[", "]", "(", ")", ".",
        "*", "+", "?", "|", "<", ">", "-", "&", "%", "@"};

    for (int i = 0; i < specialCharacters.length; i++) {
      if (inputString.contains(specialCharacters[i])) {
        inputString = inputString.replace(specialCharacters[i], "\\" + specialCharacters[i]);
      }
    }
    return inputString;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  public void unparseSqlIntervalLiteralHive(SqlWriter writer,
      SqlIntervalLiteral literal) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    if (interval.getSign() == -1) {
      writer.print("-");
    }
    writer.literal(literal.getValue().toString());
  }

  @Override public void unparseSqlDatetimeArithmetic(SqlWriter writer,
      SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {
    switch (sqlKind) {
    case MINUS:
      final SqlWriter.Frame dateDiffFrame = writer.startFunCall("DATEDIFF");
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(dateDiffFrame);
      break;
    }
  }

  @Override public void unparseIntervalOperandsBasedFunctions(SqlWriter writer,
      SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(call.getOperator().toString());
    writer.sep(",");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    unparseSqlIntervalLiteralHive(writer, call.operand(1));
    writer.endFunCall(frame);
  }

  private void unparseNullIf(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlNode[] operands = new SqlNode[call.getOperandList().size()];
    call.getOperandList().toArray(operands);
    SqlParserPos pos = call.getParserPosition();
    SqlNode[] ifOperands = new SqlNode[]{new SqlBasicCall(EQUALS, operands, pos),
        SqlLiteral.createNull(SqlParserPos.ZERO), operands[0]};
    SqlCall ifCall = new SqlBasicCall(IF, ifOperands, pos);
    unparseCall(writer, ifCall, leftPrec, rightPrec);
  }

}
// End HiveSqlDialect.java

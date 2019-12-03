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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * This Utility class contains the unparse logic that is common for more than one dialect
 */

public abstract class UnparseCommonUtil {

  /**
   * This Utility method contains common portion of HIVE and Spark for DATE_ADD
   */

  public static void unparseHiveSparkDateAddAndSub(SqlCall call, SqlWriter writer,
      int leftPrec, int rightPrec) {
    switch (call.operand(1).getKind()) {
    case LITERAL:
    case TIMES:
      SqlCall dateAddCall = makeDateAddCall(call, writer);
      call.getOperator().unparse(writer, dateAddCall, leftPrec, rightPrec);
      break;
    default:
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
    }
  }

  private static SqlCall makeDateAddCall(SqlCall call, SqlWriter writer) {
    SqlNode intervalValue = modifyIntervalCall(writer, call.operand(1));
    SqlNode[] sqlNodes = new SqlNode[]{call.operand(0), intervalValue};
    return new SqlBasicCall(call.getOperator(), sqlNodes, SqlParserPos.ZERO);
  }

  private static SqlNode modifyIntervalCall(SqlWriter writer, SqlNode intervalOperand) {

    if (intervalOperand.getKind() == SqlKind.LITERAL) {
      return modifiedIntervalForLiteral(writer, intervalOperand);
    }
    return modifiedIntervalForBasicCall(writer, intervalOperand);
  }

  private static SqlNode modifiedIntervalForBasicCall(SqlWriter writer, SqlNode intervalOperand) {
    SqlLiteral intervalLiteralValue = getLiteralValue(intervalOperand);
    SqlNode identifierValue = getIdentifierValue(intervalOperand);
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) intervalLiteralValue.getValue();
    writeNegativeLiteral(interval, writer);
    if (interval.getIntervalLiteral().equals("1")) {
      return identifierValue;
    }
    SqlNode intervalValue = new SqlIdentifier(interval.toString(),
        intervalOperand.getParserPosition());
    SqlNode[] sqlNodes = new SqlNode[]{identifierValue,
        intervalValue};
    return new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, sqlNodes, SqlParserPos.ZERO);
  }

  public static SqlLiteral getLiteralValue(SqlNode intervalOperand) {
    if ((((SqlBasicCall) intervalOperand).operand(1).getKind() == SqlKind.IDENTIFIER)
        || (((SqlBasicCall) intervalOperand).operand(1) instanceof SqlNumericLiteral)) {
      return ((SqlBasicCall) intervalOperand).operand(0);
    }
    return ((SqlBasicCall) intervalOperand).operand(1);
  }

  public static SqlNode getIdentifierValue(SqlNode intervalOperand) {
    if (((SqlBasicCall) intervalOperand).operand(1).getKind() == SqlKind.IDENTIFIER
        || (((SqlBasicCall) intervalOperand).operand(1) instanceof SqlNumericLiteral)) {
      return ((SqlBasicCall) intervalOperand).operand(1);
    }
    return ((SqlBasicCall) intervalOperand).operand(0);
  }

  private static SqlNode modifiedIntervalForLiteral(SqlWriter writer, SqlNode intervalOperand) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) intervalOperand).getValue();
    writeNegativeLiteral(interval, writer);
    return new SqlIdentifier(interval.toString(), intervalOperand.getParserPosition());
  }

  private static void writeNegativeLiteral(SqlIntervalLiteral.IntervalValue interval,
      SqlWriter writer) {
    if (interval.signum() == -1) {
      writer.print("-");
    }
  }
}



// End UnparseCommonUtil.java

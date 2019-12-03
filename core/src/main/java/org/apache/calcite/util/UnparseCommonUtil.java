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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * This Utility class contains the unparse logic that is common for more than one dialect
 */

public abstract class UnparseCommonUtil {

  /**
   * This Utility method contains common portion of  HIVE and Spark for DATE_ADD
   */

  public static SqlCall makeDateAddCall(SqlCall call, SqlWriter writer) {
    SqlNode intervalValue = UnparseCommonUtil.modifyIntervalCall(writer, call.operand(1));
    SqlNode[] sqlNodes = new SqlNode[]{call.operand(0), intervalValue};
    return new SqlBasicCall(call.getOperator(), sqlNodes, SqlParserPos.ZERO);
  }

  public static SqlNode modifyIntervalCall(SqlWriter writer,
      SqlNode intervalOperand) {
    SqlIntervalLiteral.IntervalValue interval;
    if (intervalOperand.getKind() == SqlKind.LITERAL) {
      SqlLiteral literalValue = (SqlLiteral) intervalOperand;
      if (literalValue.getTypeName().getFamily() == SqlTypeFamily.NUMERIC) {
        return new SqlIdentifier(literalValue.toString(), intervalOperand.getParserPosition());
      }
      interval =
          (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) intervalOperand).getValue();
      if (interval.signum() == -1) {
        writer.print("-");
      }
      return new SqlIdentifier(interval.toString(), intervalOperand.getParserPosition());
    } else {
      SqlIntervalLiteral intervalLiteral =
          (SqlIntervalLiteral) ((SqlBasicCall) intervalOperand).operand(1);
      interval = (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
      if (interval.signum() == -1) {
        writer.print("-");
      }
      if (interval.getIntervalLiteral().equals("1")) {
        return ((SqlBasicCall) intervalOperand).operand(0);
      }
      SqlNode intervalValue = new SqlIdentifier(interval.toString(),
          intervalOperand.getParserPosition());
      SqlNode[] sqlNodes = new SqlNode[]{((SqlBasicCall) intervalOperand).operand(0),
          intervalValue};
      return new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, sqlNodes, SqlParserPos.ZERO);
    }
  }
}

// End UnparseCommonUtil.java

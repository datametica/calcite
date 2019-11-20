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
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.regex.Pattern;

/**
 * A <code>ToNumberUtils</code> implementation for the ToNumber method.
 */
public class ToNumberUtils {

  private ToNumberUtils() {
  }

  private static void handleCasting(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
      SqlTypeName sqlTypeName) {
    call.setOperand(1,
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(sqlTypeName, SqlParserPos.ZERO),
            SqlParserPos.ZERO));
    SqlNode[] extractNodeOperands = new SqlNode[]{call.operand(0), call.operand(1)};
    SqlCall extractCallCast = new SqlBasicCall(SqlStdOperatorTable.CAST,
        extractNodeOperands, SqlParserPos.ZERO);

    SqlStdOperatorTable.CAST.unparse(writer, extractCallCast, leftPrec, rightPrec);
  }

  private static void handleNegativeValue(SqlCall call, String regEx) {
    String firstOperand = call.operand(0).toString().replaceAll(regEx, "");
    if (call.operand(1).toString().contains("MI") || call.operand(1).toString().contains("S")) {
      firstOperand = "-" + firstOperand;
    }
    call.setOperand(0,
        new SqlBasicCall(SqlStdOperatorTable.LITERAL_CHAIN, new SqlNode[]{
            SqlLiteral.createCharString(firstOperand,
                call.operand(1).getParserPosition())}, SqlParserPos.ZERO));
  }

  public static void handleToNumber(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperandList().size()) {
    case 2:
      if (Pattern.matches("^'[Xx]+'", call.operand(1).toString())) {
        SqlNode[] sqlNodes = new SqlNode[]{new SqlBasicCall(SqlStdOperatorTable.LITERAL_CHAIN,
            new SqlNode[]{SqlLiteral.createCharString("0x", call.operand(1).getParserPosition())},
            SqlParserPos.ZERO), call.operand(0)};
        SqlCall extractCall = new SqlBasicCall(SqlStdOperatorTable.CONCAT, sqlNodes,
            SqlParserPos.ZERO);
        call.setOperand(0, extractCall);

        ToNumberUtils.handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.INTEGER);

      } else if (call.operand(0).toString().contains(".")) {
        String regEx = "[-']+";
        handleNegativeValue(call, regEx);
        handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.FLOAT);
      } else {
        String regEx = "[-',$]+";
        handleNegativeValue(call, regEx);

        handleCasting(writer, call, leftPrec, rightPrec, (call.operand(0).toString
            ().contains("E")
            && call.operand(1).toString().contains("E"))
            ? SqlTypeName.DECIMAL : SqlTypeName.INTEGER);
      }
      break;
    case 3:
      if (call.operand(1).toString().replaceAll("[L']+", "").length()
          + call.operand(2).toString().split("=")[1].replaceAll("[']+", "").length()
          == call.operand(0).toString().replaceAll("[']+", "").length()) {

        int lengthOfFirstArgument = call.operand(0).toString().replaceAll("[']+", "").length()
            - call.operand(1).toString().replaceAll("[L']+", "").length();

        call.setOperand(0,
            new SqlBasicCall(SqlStdOperatorTable.LITERAL_CHAIN, new SqlNode[]{
                SqlLiteral.createCharString(call.operand(0).toString().replaceAll("[']+", "")
                        .substring(lengthOfFirstArgument, call.operand(0)
                            .toString().replaceAll("[']+", "").length()),
                    call.operand(1).getParserPosition())}, SqlParserPos.ZERO));

        ToNumberUtils.handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.INTEGER);

      }
      break;
    default:
      throw new IllegalArgumentException("Illegal Argument Exception");
    }
  }

}
// End ToNumberUtils.java

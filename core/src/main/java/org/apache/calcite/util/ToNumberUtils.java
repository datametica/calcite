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
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.regex.Pattern;

/**
 * This class is specific to BigQuery, Hive and Spark.
 */
public class ToNumberUtils {

  private ToNumberUtils() {
  }

  private static String regExRemove = "[',$A-Za-z]+";

  private static void handleCasting(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec,
      SqlTypeName sqlTypeName) {
    SqlNode[] extractNodeOperands = new SqlNode[]{call.operand(0), new SqlDataTypeSpec(new
        SqlBasicTypeNameSpec(sqlTypeName, SqlParserPos.ZERO),
        SqlParserPos.ZERO)};
    SqlCall extractCallCast = new SqlBasicCall(SqlStdOperatorTable.CAST,
        extractNodeOperands, SqlParserPos.ZERO);

    SqlStdOperatorTable.CAST.unparse(writer, extractCallCast, leftPrec, rightPrec);
  }

  private static void modifyOperand(SqlCall call) {
    String regEx = "[',$]+";
    if (call.operand(1).toString().contains("C")) {
      regEx = "[',$A-Za-z]+";
    }

    String firstOperand = call.operand(0).toString().replaceAll(regEx, "");
    if (call.operand(1).toString().contains("MI") || call.operand(1).toString().contains("S")) {
      firstOperand = firstOperand.replaceAll("-", "");
      firstOperand = "-" + firstOperand;
    }

    SqlNode[] sqlNode = new SqlNode[]{SqlLiteral.createCharString(firstOperand.trim(),
        SqlParserPos.ZERO)};
    call.setOperand(0, sqlNode[0]);
  }

  private static boolean handleNullOperand(
      SqlWriter writer, int leftPrec, int rightPrec) {
    SqlNode[] extractNodeOperands = new SqlNode[]{new SqlDataTypeSpec(new
        SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
        SqlParserPos.ZERO), new SqlDataTypeSpec(new
        SqlBasicTypeNameSpec(SqlTypeName.INTEGER, SqlParserPos.ZERO),
        SqlParserPos.ZERO)};

    SqlCall extractCallCast = new SqlBasicCall(SqlStdOperatorTable.CAST,
        extractNodeOperands, SqlParserPos.ZERO);

    SqlStdOperatorTable.CAST.unparse(writer, extractCallCast, leftPrec, rightPrec);
    return true;
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

  public static void unparseToNumbertoConv(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {

    SqlNode[] sqlNode = new SqlNode[]{call.getOperandList().get(0), SqlLiteral.createExactNumeric
        ("16",
        SqlParserPos.ZERO), SqlLiteral.createExactNumeric("10",
        SqlParserPos.ZERO)};

    SqlCall extractCall = new SqlBasicCall(SqlStdOperatorTable.CONV, sqlNode,
        SqlParserPos.ZERO);
    call.setOperand(0, extractCall);
    handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.BIGINT);
  }

  public static void unparseToNumber(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperandList().size()) {
    case 1:
    case 3:
      if ((call.operand(0) instanceof SqlCharStringLiteral || call.operand(0)
          instanceof SqlLiteral) && isOperandNull(call)) {
        handleNullOperand(writer, leftPrec, rightPrec);
      } else {
        if (call.operand(0) instanceof SqlCharStringLiteral) {
          String firstOperand = call.operand(0).toString().replaceAll(regExRemove, "");
          SqlNode[] sqlNode = new SqlNode[]{SqlLiteral.createCharString(firstOperand.trim(),
              SqlParserPos.ZERO)};
          call.setOperand(0, sqlNode[0]);
        }

        SqlTypeName sqlTypeName = call.operand(0).toString().contains(".")
            ? SqlTypeName.FLOAT : SqlTypeName.BIGINT;
        handleCasting(writer, call, leftPrec, rightPrec, sqlTypeName);
      }
      break;
    case 2:
      if ((call.operand(0) instanceof SqlCharStringLiteral || call.operand(0)
          instanceof SqlLiteral) && isOperandNull(call)) {
        handleNullOperand(writer, leftPrec, rightPrec);
      } else {
        if (Pattern.matches("^'[Xx]+'", call.operand(1).toString())) {
          SqlNode[] sqlNodes = new SqlNode[]{SqlLiteral.createCharString("0x",
              SqlParserPos.ZERO), call.operand(0)};
          SqlCall extractCall = new SqlBasicCall(SqlStdOperatorTable.CONCAT, sqlNodes,
              SqlParserPos.ZERO);
          call.setOperand(0, extractCall);
          handleCasting(writer, call, leftPrec, rightPrec, SqlTypeName.BIGINT);

        } else {
          SqlTypeName sqlType;
          if (call.operand(0).toString().contains(".")) {
            sqlType = SqlTypeName.FLOAT;
          } else {
            sqlType = call.operand(0).toString().contains("E")
                && call.operand(1).toString().contains("E")
                ? SqlTypeName.DECIMAL : SqlTypeName.BIGINT;
          }
          modifyOperand(call);
          handleCasting(writer, call, leftPrec, rightPrec, sqlType);
        }
      }
      break;
    default:
      throw new IllegalArgumentException("Illegal Argument Exception");
    }
  }

}
// End ToNumberUtils.java

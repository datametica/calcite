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
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_FORMAT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;

/**
 * This class is specific to Hive and Spark to unparse CURRENT_TIMESTAMP function
 */
public class CurrentTimestampUtils {

  private CurrentTimestampUtils() {
  }

  public static void unparseCurrentTimestamp(
          SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlCharStringLiteral formatNode = makeDateFormatSqlCall(call);
    SqlNode timestampCall = new SqlBasicCall(CURRENT_TIMESTAMP, SqlNode.EMPTY_ARRAY,
        SqlParserPos.ZERO) {
      @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        SqlSyntax.FUNCTION.unparse(writer, CURRENT_TIMESTAMP, getEmptyCall(), leftPrec, rightPrec);
      }
    };
    SqlNode[] formatTimestampOperands = new SqlNode[]{timestampCall, formatNode};
    SqlCall dateFormatCall = new SqlBasicCall(DATE_FORMAT, formatTimestampOperands,
        SqlParserPos.ZERO);
    DATE_FORMAT.unparse(writer, dateFormatCall, leftPrec, rightPrec);
  }

  private static SqlBasicCall getEmptyCall() {
    return new SqlBasicCall(CURRENT_TIMESTAMP, SqlBasicCall.EMPTY_ARRAY, SqlParserPos.ZERO);
  }

  private static SqlCharStringLiteral makeDateFormatSqlCall(SqlCall call) {
    String precision = call.operandCount() > 0
            ? ((SqlLiteral) call.operand(0)).getValue().toString() : "6";
    String fractionPart = getSecondFractionPart(precision);
    String dateFormat = "yyyy-MM-dd HH:mm:ss" + fractionPart;
    return SqlLiteral.createCharString(dateFormat, SqlParserPos.ZERO);
  }

  private static String getSecondFractionPart(String precision) {
    switch (precision) {
    case "0":
      return "";
    case "1":
      return ".s";
    case "2":
      return ".ss";
    case "3":
      return ".sss";
    case "4":
      return ".ssss";
    case "5":
      return ".sssss";
    case "6":
      return ".ssssss";
    default:
      throw new AssertionError("Handling of precision: " + precision + "not available in HiveSQL "
          + "Dialect");
    }
  }
}

// End CurrentTimestampUtils.java

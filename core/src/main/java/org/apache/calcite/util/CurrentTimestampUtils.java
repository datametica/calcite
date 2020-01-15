package org.apache.calcite.util;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_FORMAT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;

/**
 * This class is specific to Hive and Spark to unparse CURRENT_TIMESTAMP function
 */
public class CurrentTimestampUtils {

  private CurrentTimestampUtils() {
  }

  public static void unparseCurrentTimestamp(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlCharStringLiteral formatNode = makeDateFormatSqlCall(call);
    SqlNode timestampCall = new SqlBasicCall(CURRENT_TIMESTAMP, SqlNode.EMPTY_ARRAY, SqlParserPos.ZERO) {
      @Override
      public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        SqlSyntax.FUNCTION.unparse(writer, CURRENT_TIMESTAMP, getEmptyCall(), leftPrec, rightPrec);
      }
    };
    SqlNode[] formatTimestampOperands = new SqlNode[]{timestampCall, formatNode};
    SqlCall dateFormatCall = new SqlBasicCall(DATE_FORMAT, formatTimestampOperands, SqlParserPos.ZERO);
    DATE_FORMAT.unparse(writer, dateFormatCall, leftPrec, rightPrec);
  }

  private static SqlBasicCall getEmptyCall() {
    return new SqlBasicCall(CURRENT_TIMESTAMP, SqlBasicCall.EMPTY_ARRAY, SqlParserPos.ZERO);
  }

  private static SqlCharStringLiteral makeDateFormatSqlCall(SqlCall call) {
    String precision = call.operandCount() > 0 ? ((SqlLiteral) call.operand(0)).getValue().toString() : "6";
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
        throw new AssertionError("Handling of precision: "
                + precision + "not available in HiveSQL Dialect");
    }
  }
}
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

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.DateString;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;

/**
 * Support unparse logic for DateTimestamp function
 */
public class DateTimestampFormatUtil {
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlCall extractCall = null;
    switch (call.getOperator().getName()) {
    case "WEEKNUMBER_OF_YEAR":
      extractCall = unparseWeekNumber(call.operand(0), DateTimeUnit.WEEK);
      break;
    case "YEARNUMBER_OF_CALENDAR":
      extractCall = unparseWeekNumber(call.operand(0), DateTimeUnit.YEAR);
      break;
    case "MONTHNUMBER_OF_YEAR":
      extractCall = unparseWeekNumber(call.operand(0), DateTimeUnit.MONTH);
      break;
    case "QUARTERNUMBER_OF_YEAR":
      extractCall = unparseWeekNumber(call.operand(0), DateTimeUnit.QUARTER);
      break;
    case "MONTHNUMBER_OF_QUARTER":
      extractCall = unparseMonthNumberQuarter(call, DateTimeUnit.MONTH);
      break;
    case "WEEKNUMBER_OF_MONTH":
      extractCall = unparseMonthNumber(call, DateTimeUnit.DAY);
      break;
    case "WEEKNUMBER_OF_CALENDAR":
      extractCall = handleWeekNumberCalendar(call, DateTimeUnit.WEEK);
      break;
    case "DAYOCCURRENCE_OF_MONTH":
      extractCall = handleDayOccurrenceMonth(call, DateTimeUnit.DAY);
      break;
    }
    if (null != extractCall) {
      extractCall.unparse(writer, leftPrec, rightPrec);
    }
  }

  private SqlCall handleDayOccurrenceMonth(SqlCall call, DateTimeUnit dateTimeUnit) {
    SqlCall divideSqlCall = handleDivideLiteral(call, dateTimeUnit);
    SqlNode[] plusOperands = new SqlNode[] { divideSqlCall, SqlLiteral.createExactNumeric("1",
        SqlParserPos.ZERO) };
    SqlCall plusSqlCall = new SqlBasicCall(SqlStdOperatorTable.PLUS, plusOperands,
        SqlParserPos.ZERO);
    BasicSqlType sqlType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER);
    return CAST.createCall(SqlParserPos.ZERO, plusSqlCall, SqlTypeUtil.convertTypeToSpec(sqlType));
  }

  private SqlCall handleWeekNumberCalendar(SqlCall call, DateTimeUnit dateTimeUnit) {
    SqlNode[] dateCastOperand = new SqlNode[] {
        SqlLiteral.createDate(new DateString("1900-01-01"), SqlParserPos.ZERO)
    };
    SqlNode[] dateDiffOperands = new SqlNode[] { call.operand(0), dateCastOperand[0],
        SqlLiteral.createSymbol(dateTimeUnit, SqlParserPos.ZERO) };
    return new SqlBasicCall(SqlLibraryOperators.DATE_DIFF, dateDiffOperands,
        SqlParserPos.ZERO);
  }

  private SqlCall unparseMonthNumber(SqlCall call, DateTimeUnit dateTimeUnit) {
    SqlCall divideSqlCall = handleDivideLiteral(call, dateTimeUnit);
    SqlNode[] floorOperands = new SqlNode[] { divideSqlCall };
    return new SqlBasicCall(SqlStdOperatorTable.FLOOR, floorOperands,
        SqlParserPos.ZERO);
  }

  private SqlCall handleDivideLiteral(SqlCall call, DateTimeUnit dateTimeUnit) {
    SqlCall extractCall = unparseWeekNumber(call.operand(0), dateTimeUnit);
    SqlNode[] divideOperands = new SqlNode[] { extractCall, SqlLiteral.createExactNumeric("7",
        SqlParserPos.ZERO)};
    return new SqlBasicCall(SqlStdOperatorTable.DIVIDE, divideOperands,
        SqlParserPos.ZERO);
  }

  /**
   * Parse week number based on value.*/
  protected SqlCall unparseWeekNumber(SqlNode operand, DateTimeUnit dateTimeUnit) {
    SqlNode[] operands = new SqlNode[] {
        SqlLiteral.createSymbol(dateTimeUnit, SqlParserPos.ZERO), operand
    };
    return new SqlBasicCall(SqlStdOperatorTable.EXTRACT, operands,
        SqlParserPos.ZERO);
  }

  private SqlCall unparseMonthNumberQuarter(SqlCall call, DateTimeUnit dateTimeUnit) {
    SqlCall extractCall = unparseWeekNumber(call.operand(0), dateTimeUnit);
    SqlNumericLiteral quarterLiteral = SqlLiteral.createExactNumeric("3",
        SqlParserPos.ZERO);
    SqlNode[] modOperand = new SqlNode[] { extractCall, quarterLiteral};
    SqlCall modSqlCall = new SqlBasicCall(SqlStdOperatorTable.MOD, modOperand, SqlParserPos.ZERO);
    SqlNode[] equalsOperands = new SqlNode[] { modSqlCall, SqlLiteral.createExactNumeric("0",
        SqlParserPos.ZERO)};
    SqlCall equalsSqlCall = new SqlBasicCall(SqlStdOperatorTable.EQUALS, equalsOperands,
        SqlParserPos.ZERO);
    SqlNode[] ifOperands = new SqlNode[] { equalsSqlCall, quarterLiteral, modSqlCall };
    return new SqlBasicCall(SqlStdOperatorTable.IF, ifOperands, SqlParserPos.ZERO);
  }

  /**
   * DateTime Unit for supporting different categories of date and time
   */
  private enum DateTimeUnit {
    DAY("DAY"),
    WEEK("WEEK"),
    DAYOFYEAR("DAYOFYEAR"),
    MONTH("MONTH"),
    MONTHOFYEAR("MONTHOFYEAR"),
    QUARTER("QUARTER"),
    YEAR("YEAR");

    String value;

    DateTimeUnit(String value) {
      this.value = value;
    }
  }
}

// End DateTimestampFormatUtil.java

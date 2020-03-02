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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import org.apache.commons.lang3.StringUtils;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_VARCHAR;

/**
 * A <code>SqlDialect</code> implementation for the Snowflake database.
 */
public class SnowflakeSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new SnowflakeSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.SNOWFLAKE)
          .withIdentifierQuoteString("\"")
          .withUnquotedCasing(Casing.TO_UPPER)
          .withConformance(SqlConformanceEnum.SNOWFLAKE));

  /** Creates a SnowflakeSqlDialect. */
  public SnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseCall(
      final SqlWriter writer, final SqlCall call, final int leftPrec,
      final int rightPrec) {
    switch (call.getKind()) {
    case FORMAT:
      unparseFormatFunction(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseFormatFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperandList().size()) {
    case 1:
      if (call.operand(0).toString().equalsIgnoreCase("null")) {
        SqlNode[] extractNodeOperands = new SqlNode[] {
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.NULL, SqlParserPos.ZERO),
                SqlParserPos.ZERO)
        };
        SqlCall sqlCall = new SqlBasicCall(TO_VARCHAR, extractNodeOperands, SqlParserPos.ZERO);
        super.unparseCall(writer, sqlCall, leftPrec, rightPrec);
      }
      break;
    case 2:
      SqlNode[] sqlNode;
      if (call.operand(1) instanceof SqlIdentifier) {
        sqlNode = handleColumnOperand(call);
      } else {
        sqlNode = handleLiteralOperand(call);
      }
      SqlCall sqlCall = new SqlBasicCall(TO_VARCHAR, sqlNode, SqlParserPos.ZERO);
      super.unparseCall(writer, sqlCall, leftPrec, rightPrec);
      break;
    }
  }

  private SqlNode[] handleLiteralOperand(SqlCall call) {
    String modifiedOperand;
    SqlNode[] sqlNode;
    if (call.operand(1).toString().contains(".")) {
      modifiedOperand = call.operand(1).toString()
          .replaceAll("[0-9]", "9")
          .replaceAll("'", "");
    } else {
      int firstOperand = Integer.valueOf(call.operand(0).toString()
          .replaceAll("[^0-9]", "")) - 1;
      modifiedOperand = StringUtils.repeat("9", firstOperand);
    }
    sqlNode = new SqlNode[]{
        SqlLiteral.createExactNumeric(
            call.operand(1).toString().replaceAll("'", ""),
            SqlParserPos.ZERO),
        SqlLiteral.createCharString(modifiedOperand.trim(),
            SqlParserPos.ZERO)};
    return sqlNode;
  }

  private SqlNode[] handleColumnOperand(SqlCall call) {
    String modifiedOperand;
    SqlNode[] sqlNode;
    if (call.operand(0).toString().contains(".")) {
      modifiedOperand = call.operand(0).toString()
          .replaceAll("%|f|'", "");
      String[] modifiedOperandArry = modifiedOperand.split("\\.");
      int intValue = Integer.valueOf(modifiedOperandArry[0]) - 1;
      modifiedOperand = StringUtils.repeat("9",
          intValue - 1 - Integer.valueOf(modifiedOperandArry[1]));
      int decimalValue = Integer.valueOf(modifiedOperandArry[1]);
      modifiedOperand += "." + StringUtils.repeat("0", decimalValue);
    } else {
      int intValue = Integer.valueOf(call.operand(0).toString()
          .replaceAll("[^0-9]", ""));
      modifiedOperand = StringUtils.repeat("9", intValue - 1);
    }
    sqlNode = new SqlNode[]{
        call.operand(1),
        SqlLiteral.createCharString(modifiedOperand.trim(),
            SqlParserPos.ZERO)};
    return sqlNode;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }
}

// End SnowflakeSqlDialect.java

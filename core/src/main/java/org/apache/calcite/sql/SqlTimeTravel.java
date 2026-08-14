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
package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree node that represents a time-travel table reference.
 *
 * <p>Syntax:
 * <blockquote><pre>{@code
 * table_name FOR SYSTEM_TIME AS OF (timestamp_expression)
 * }</pre></blockquote>
 */
public class SqlTimeTravel extends SqlCall {

  private final SqlIdentifier tableIdentifier;
  private final String timeTravelType;
  private final SqlNode periodNode;
  private final SqlIdentifier alias;

  public static final SqlOperator TIME_TRAVEL =
      new SqlSpecialOperator("TIME_TRAVEL", SqlKind.OTHER);

  SqlTimeTravel(SqlIdentifier tableIdentifier, SqlIdentifier alias, String timeTravelType,
      SqlNode periodNode) {
    super(SqlParserPos.ZERO);
    this.tableIdentifier = tableIdentifier;
    this.alias = alias;
    this.timeTravelType = timeTravelType;
    this.periodNode = periodNode;
  }

  @Override public SqlOperator getOperator() {
    return TIME_TRAVEL;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableIdentifier, periodNode);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (!(writer.getDialect() instanceof BigQuerySqlDialect)) {
      throw new UnsupportedOperationException(writer.getDialect() + " This dialect's are not "
          + "handled TIME TRAVEL");
    } else {
      tableIdentifier.unparse(writer, leftPrec, rightPrec);
      if (alias != null) {
        writer.keyword("AS");
        alias.unparse(writer, 0, 0);
      }
      writer.keyword("FOR SYSTEM_TIME AS OF");
      writer.print("(");
      periodNode.unparse(writer, 0, 0);
      writer.print(")");
    }
  }
}

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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_FORMAT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;

/**
 * This class is specific to Hive and Spark to unparse CURRENT_TIMESTAMP function
 */
public class CurrentTimestampUtils {

  private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private CurrentTimestampUtils() {
  }

  public static SqlBasicCall makeDateFormatCall(SqlCall call) {
    SqlCharStringLiteral formatNode = makeDateFormatSqlCall(call);
    SqlNode timestampCall = new SqlBasicCall(CURRENT_TIMESTAMP, SqlNode.EMPTY_ARRAY,
            SqlParserPos.ZERO);
    SqlNode[] formatTimestampOperands = new SqlNode[]{timestampCall, formatNode};
    return new SqlBasicCall(DATE_FORMAT, formatTimestampOperands,
        SqlParserPos.ZERO);
  }

  private static SqlCharStringLiteral makeDateFormatSqlCall(SqlCall call) {
    String precision = ((SqlLiteral) call.operand(0)).getValue().toString();
    String fractionPart = StringUtils.repeat("s", Integer.parseInt(precision));
    return SqlLiteral.createCharString
            (buildDatetimeFormat(precision, fractionPart), SqlParserPos.ZERO);
  }

  private static String buildDatetimeFormat(String precision, String fractionPart) {
    return Integer.parseInt(precision) > 0
            ? DEFAULT_DATE_FORMAT + "." + fractionPart : DEFAULT_DATE_FORMAT;
  }
}

// End CurrentTimestampUtils.java

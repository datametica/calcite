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
package org.apache.calcite.util.interval;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;

/**
 * Date interval handling
 */
public class DateIntervalUtil {

  public SqlIntervalLiteral updateSqlIntervalLiteral(SqlIntervalLiteral literal) {
    SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    switch (literal.getTypeName()) {
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_DAY_SECOND:
      long equivalentSecondValue = SqlParserUtil.intervalToMillis(interval.getIntervalLiteral(),
          interval.getIntervalQualifier()) / 1000;
      SqlIntervalQualifier qualifier = new SqlIntervalQualifier(TimeUnit.SECOND,
          RelDataType.PRECISION_NOT_SPECIFIED, TimeUnit.SECOND,
          RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO);
      return SqlLiteral.createInterval(interval.getSign(), Long.toString(equivalentSecondValue),
          qualifier, literal.getParserPosition());
    case INTERVAL_DAY_MINUTE:
      equivalentSecondValue = SqlParserUtil.intervalToMillis(interval.getIntervalLiteral(),
          interval.getIntervalQualifier()) / 1000;
      qualifier = new SqlIntervalQualifier(TimeUnit.MINUTE,
          RelDataType.PRECISION_NOT_SPECIFIED, TimeUnit.MINUTE,
          RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO);
      return SqlLiteral.createInterval(interval.getSign(), Long.toString(equivalentSecondValue),
          qualifier, literal.getParserPosition());
    case INTERVAL_YEAR_MONTH:
      equivalentSecondValue = SqlParserUtil.intervalToMonths(interval.getIntervalLiteral(),
          interval.getIntervalQualifier());
      qualifier = new SqlIntervalQualifier(TimeUnit.MONTH,
          RelDataType.PRECISION_NOT_SPECIFIED, TimeUnit.MONTH,
          RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO);
      return SqlLiteral.createInterval(interval.getSign(), Long.toString(equivalentSecondValue),
          qualifier, literal.getParserPosition());
    case INTERVAL_DAY_HOUR:
      equivalentSecondValue = SqlParserUtil.intervalToMonths(interval.getIntervalLiteral(),
          interval.getIntervalQualifier());
      /*equivalentSecondValue = interval.getIntervalQualifier()
      .evaluateIntervalLiteral(interval.getIntervalLiteral(),
          interval.getIntervalQualifier().getParserPosition(), RelDataTypeSystem.DEFAULT);*/
      qualifier = new SqlIntervalQualifier(TimeUnit.HOUR,
          RelDataType.PRECISION_NOT_SPECIFIED, TimeUnit.HOUR,
          RelDataType.PRECISION_NOT_SPECIFIED, SqlParserPos.ZERO);
      return SqlLiteral.createInterval(interval.getSign(), Long.toString(equivalentSecondValue),
          qualifier, literal.getParserPosition());
    default:
      return literal;
    }
  }
}

// End DateIntervalUtil.java

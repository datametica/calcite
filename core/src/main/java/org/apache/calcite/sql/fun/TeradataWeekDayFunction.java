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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;

/**
 * Operator identity for the Teradata weekday calendar functions (TD_SUNDAY, TD_MONDAY,
 * TD_TUESDAY, TD_WEDNESDAY, TD_THURSDAY, TD_FRIDAY, TD_SATURDAY). Per the Teradata SQL
 * reference the syntax is {@code TD_SUNDAY(expression_1 [, calendar_name])}, where
 * {@code expression_1} evaluates to a DATE, TIMESTAMP or TIMESTAMP WITH TIME ZONE and the
 * optional {@code calendar_name} is a character literal ({@code 'Teradata'}, {@code 'ISO'}
 * or {@code 'COMPATIBLE'}); when it is omitted the session calendar is used. Each function
 * returns the occurrence of that weekday falling immediately before {@code expression_1}.
 *
 * <p>Each instance carries its weekday so that dialect-specific unparse logic can call
 * {@link #getWeekDay()} directly instead of re-parsing it out of the operator name.
 */
public class TeradataWeekDayFunction extends SqlFunction {
  private static final List<String> WEEK_DAYS =
      ImmutableList.of("SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY",
          "THURSDAY", "FRIDAY", "SATURDAY");

  private final String weekDay;

  private TeradataWeekDayFunction(String functionName, String weekDay) {
    super(functionName, SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE, null,
        OperandTypes.or(
        OperandTypes.DATETIME_OPTIONAL_STRING_OPTIONAL_TIME,
        OperandTypes.TIMESTAMP_LTZ),
        SqlFunctionCategory.TIMEDATE);
    this.weekDay = weekDay;
  }

  public String getWeekDay() {
    return weekDay;
  }

  public static TeradataWeekDayFunction of(String functionName) {
    if (functionName.trim().isEmpty()) {
      throw new IllegalArgumentException("functionName must not be empty");
    }
    String normalized = functionName.toUpperCase(Locale.ROOT);
    String day = normalized.replace("TD_", "");
    if (!WEEK_DAYS.contains(day)) {
      throw new IllegalArgumentException(
          "Teradata Calendar Function " + functionName + " is not supported");
    }
    return new TeradataWeekDayFunction(normalized, day);
  }
}

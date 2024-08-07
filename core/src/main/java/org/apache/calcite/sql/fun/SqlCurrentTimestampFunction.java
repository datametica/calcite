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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Sub class of SqlAbstractTimeFunction for current_timestamp function such as CURRENT_TIMESTAMP(6)
 * or CURRENT_TIMESTAMP".
 */
public class SqlCurrentTimestampFunction extends SqlAbstractTimeFunction {

  public final SqlTypeName typeName;

  public SqlCurrentTimestampFunction(String name, SqlTypeName typeName) {
    super(name, typeName);
    this.typeName = typeName;
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    int precision = 0;
    if (opBinding.getOperandCount() == 1) {
      RelDataType type = opBinding.getOperandType(0);
      if (SqlTypeUtil.isNumeric(type)) {
        precision = opBinding.getOperandLiteralValue(0, Integer.class);
      }
    }
    assert precision >= 0;
    int maximumPrecision = opBinding.getTypeFactory().getTypeSystem().getMaxPrecision(typeName);
    if (precision > maximumPrecision) {
      throw opBinding.newError(
          RESOURCE.argumentMustBeValidPrecision(
              opBinding.getOperator().getName(), 0,
              maximumPrecision));
    }
    return opBinding.getTypeFactory().createSqlType(typeName, precision);
  }
}

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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.Arrays;

import static org.apache.calcite.sql.type.NonNullableAccessors.getComponentTypeOrThrow;
import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getOperandLiteralValueOrThrow;

import static java.util.Objects.requireNonNull;

/**
 * The item operator {@code [ ... ]}, used to access a given element of an
 * array, map or struct. For example, {@code myArray[3]}, {@code "myMap['foo']"},
 * {@code myStruct[2]} or {@code myStruct['fieldName']}.
 */
public class SqlItemOperator extends SqlSpecialOperator {

  private boolean isSafe;
  private static final SqlSingleOperandTypeChecker ARRAY_OR_MAP =
      OperandTypes.or(
          OperandTypes.family(SqlTypeFamily.ARRAY),
          OperandTypes.family(SqlTypeFamily.MAP),
          OperandTypes.family(SqlTypeFamily.ANY));

  public SqlItemOperator() {
    super("ITEM", SqlKind.ITEM, 100, true, null, null, null);
    this.isSafe = false;
  }

  public SqlItemOperator(boolean isSafe) {
    super("ITEM", SqlKind.ITEM, 100, true, null, null, null);
    this.isSafe = isSafe;
  }

  @Override public ReduceResult reduceExpr(int ordinal,
      TokenSequence list) {
    SqlNode left = list.node(ordinal - 1);
    SqlNode right = list.node(ordinal + 1);
    return new ReduceResult(ordinal - 1,
        ordinal + 2,
        createCall(
            SqlParserPos.sum(
                Arrays.asList(requireNonNull(left, "left").getParserPosition(),
                    requireNonNull(right, "right").getParserPosition(),
                    list.pos(ordinal))),
            left,
            right));
  }

  @Override public void unparse(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    call.operand(0).unparse(writer, leftPrec, 0);
    final SqlWriter.Frame frame = writer.startList("[", "]");
    call.operand(1).unparse(writer, 0, 0);
    writer.endList(frame);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlNode left = callBinding.operand(0);
    final SqlNode right = callBinding.operand(1);
    if (!ARRAY_OR_MAP.checkSingleOperandType(callBinding, left, 0,
        throwOnFailure)) {
      return false;
    }
    final SqlSingleOperandTypeChecker checker = getChecker(callBinding);
    return checker.checkSingleOperandType(callBinding, right, 0,
        throwOnFailure);
  }

  private static SqlSingleOperandTypeChecker getChecker(SqlCallBinding callBinding) {
    final RelDataType operandType = callBinding.getOperandType(0);
    switch (operandType.getSqlTypeName()) {
    case ARRAY:
      return OperandTypes.family(SqlTypeFamily.INTEGER);
    case MAP:
      RelDataType keyType = requireNonNull(operandType.getKeyType(),
          "operandType.getKeyType()");
      SqlTypeName sqlTypeName = keyType.getSqlTypeName();
      return OperandTypes.family(
          requireNonNull(sqlTypeName.getFamily(),
              () -> "keyType.getSqlTypeName().getFamily() null, type is " + sqlTypeName));
    case ROW:
    case ANY:
    case DYNAMIC_STAR:
      return OperandTypes.or(
          OperandTypes.family(SqlTypeFamily.INTEGER),
          OperandTypes.family(SqlTypeFamily.CHARACTER));
    default:
      throw callBinding.newValidationSignatureError();
    }
  }

  @Override public String getAllowedSignatures(String name) {
    return "<ARRAY>[<INTEGER>]\n"
        + "<MAP>[<ANY>]\n"
        + "<ROW>[<CHARACTER>|<INTEGER>]";
  }

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType operandType = opBinding.getOperandType(0);
    switch (operandType.getSqlTypeName()) {
    case ARRAY:
      return typeFactory.createTypeWithNullability(
          getComponentTypeOrThrow(operandType), true);
    case MAP:
      return typeFactory.createTypeWithNullability(
          requireNonNull(operandType.getValueType(),
              () -> "operandType.getValueType() is null for " + operandType),
          true);
    case ROW:
      RelDataType fieldType;
      RelDataType indexType = opBinding.getOperandType(1);

      if (SqlTypeUtil.isString(indexType)) {
        final String fieldName = getOperandLiteralValueOrThrow(opBinding, 1, String.class);
        RelDataTypeField field = operandType.getField(fieldName, false, false);
        if (field == null) {
          throw new AssertionError("Cannot infer type of field '"
              + fieldName + "' within ROW type: " + operandType);
        } else {
          fieldType = field.getType();
        }
      } else if (SqlTypeUtil.isIntType(indexType)) {
        Integer index = opBinding.getOperandLiteralValue(1, Integer.class);
        if (index == null || index < 1 || index > operandType.getFieldCount()) {
          throw new AssertionError("Cannot infer type of field at position "
              + index + " within ROW type: " + operandType);
        } else {
          fieldType = operandType.getFieldList().get(index - 1).getType(); // 1 indexed
        }
      } else {
        throw new AssertionError("Unsupported field identifier type: '"
            + indexType + "'");
      }
      if (fieldType != null && operandType.isNullable()) {
        fieldType = typeFactory.createTypeWithNullability(fieldType, true);
      }
      return fieldType;
    case ANY:
    case DYNAMIC_STAR:
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.ANY), true);
    default:
      throw new AssertionError();
    }
  }

  public boolean isSafe() {
    return this.isSafe;
  }
}

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

import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

/**
 * SqlSetOperator represents a relational set theory operator (UNION, INTERSECT,
 * MINUS). These are binary operators, but with an extra boolean attribute
 * tacked on for whether to remove duplicates (e.g. UNION ALL does not remove
 * duplicates).
 */
public class SqlSetOperator extends SqlBinaryOperator {
  //~ Instance fields --------------------------------------------------------

  private final boolean all;

  //~ Constructors -----------------------------------------------------------

  public SqlSetOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean all) {
    super(
        name,
        kind,
        prec,
        true,
        ReturnTypes.LEAST_RESTRICTIVE,
        null,
        OperandTypes.SET_OP);
    this.all = all;
  }

  public SqlSetOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean all,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        prec,
        true,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
    this.all = all;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean isAll() {
    return all;
  }

  public boolean isDistinct() {
    return !all;
  }

  @Override public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    validator.validateQuery(call, operandScope, validator.getUnknownType());
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    assert call.operandCount() == 2;
    SqlSetOperator rootOp = (SqlSetOperator) call.getOperator();
    List<SqlNode> operands = new ArrayList<>();
    flatten(call, rootOp, operands);
    final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SETOP);
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0) {
        writer.sep(getSetOperatorSql(rootOp));
      }
      int prec = rootOp.getLeftPrec();
      operands.get(i).unparse(writer, prec, prec);
    }
    writer.endList(frame);
  }

  private void flatten(SqlNode node, SqlSetOperator rootOp, List<SqlNode> operands) {
    if (node instanceof SqlCall) {
      SqlCall call = (SqlCall) node;
      SqlOperator op = call.getOperator();
      if (op instanceof SqlSetOperator && sameSetOp(rootOp, (SqlSetOperator) op)) {
        flatten(call.operand(0), rootOp, operands);
        flatten(call.operand(1), rootOp, operands);
        return;
      }
    }
    operands.add(node);
  }

  private boolean sameSetOp(SqlSetOperator a, SqlSetOperator b) {
    return a.getKind() == b.getKind() && a.isAll() == b.isAll();
  }

  private String getSetOperatorSql(SqlSetOperator op) {
    switch (op.getKind()) {
    case UNION:
      return op.isAll() ? "UNION ALL" : "UNION";
    case INTERSECT:
      return op.isAll() ? "INTERSECT ALL" : "INTERSECT";
    case EXCEPT:
      return op.isAll() ? "EXCEPT ALL" : "EXCEPT";
    default:
      throw new AssertionError("Unsupported set operator: " + op.getKind());
    }
  }
}

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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
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
    List<SetOpItem> items = new ArrayList<>();
    ArrayDeque<SqlCall> stack = new ArrayDeque<>();
    stack.push(call);
    while (!stack.isEmpty()) {
      SqlCall c = stack.pop();
      SqlOperator operator = c.getOperator();
      if (operator instanceof SqlSetOperator) {
        SqlSetOperator setOp = (SqlSetOperator) operator;
        SqlNode right = c.operand(1);
        SqlNode left = c.operand(0);
        if (right instanceof SqlCall
            && ((SqlCall) right).getOperator() instanceof SqlSetOperator
            && sameSetOp(setOp, (SqlSetOperator) ((SqlCall) right).getOperator())) {
          stack.push((SqlCall) right);
        } else {
          items.add(new SetOpItem(right, setOp));
        }
        if (left instanceof SqlCall
            && ((SqlCall) left).getOperator() instanceof SqlSetOperator
            && sameSetOp(setOp, (SqlSetOperator) ((SqlCall) left).getOperator())) {
          stack.push((SqlCall) left);
        } else {
          items.add(new SetOpItem(left, null));
        }
      }
    }
    Collections.reverse(items);
    SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SETOP);
    boolean first = true;
    for (SetOpItem item : items) {
      if (!first) {
        writer.sep(getSetOperatorSql(item.operator));
      }
      int prec = call.getOperator().getLeftPrec();
      item.node.unparse(writer, prec, prec);
      first = false;
    }
    writer.endList(frame);
  }

  /**
   * Holds a SQL node and the set operator.
   */
  static class SetOpItem {
    final SqlNode node;
    final SqlSetOperator operator;
    SetOpItem(SqlNode node, SqlSetOperator operator) {
      this.node = node;
      this.operator = operator;
    }
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

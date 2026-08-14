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

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    // A set-operator chain is built left-deep: ((a UNION ALL b) UNION ALL c) ...
    // The inherited SqlBinaryOperator.unparse recurses into operand(0) for every node
    // in that chain, so a chain of a few hundred nodes (e.g. a multi-row
    // INSERT ... VALUES rewritten into a UNION ALL of single-row SELECTs) overflows the
    // JVM stack with a StackOverflowError. Flatten the run of THIS same set operator into
    // a list and emit it in a single loop, so unparse depth is independent of chain
    // length. Operand precedences reproduce exactly what the nested binary unparse would
    // pass, so the rendered SQL is unchanged. (RHB-1316)
    if (call.operandCount() != 2) {
      super.unparse(writer, call, leftPrec, rightPrec);
      return;
    }
    final java.util.Deque<SqlNode> operands = new java.util.ArrayDeque<>();
    SqlNode node = call;
    while (node instanceof SqlCall
        && ((SqlCall) node).getOperator() == this
        && ((SqlCall) node).operandCount() == 2) {
      final SqlCall setCall = (SqlCall) node;
      operands.addFirst(setCall.operand(1));
      node = setCall.operand(0);
    }
    operands.addFirst(node);

    final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SETOP);
    final boolean needsSpace = needsSpace();
    final int lastIndex = operands.size() - 1;
    int index = 0;
    for (SqlNode operand : operands) {
      final int opLeftPrec = (index == 0) ? leftPrec : getRightPrec();
      final int opRightPrec = (index == lastIndex) ? rightPrec : getLeftPrec();
      if (index > 0) {
        writer.setNeedWhitespace(needsSpace);
        writer.sep(getName());
        writer.setNeedWhitespace(needsSpace);
      }
      operand.unparse(writer, opLeftPrec, opRightPrec);
      index++;
    }
    writer.endList(frame);
  }
}

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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.plan.InnerJoinTrait;
import org.apache.calcite.plan.InnerJoinTraitDef;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;

/**
 * Utility class for rel2sql package.
 */
public class RelToSqlUtils {

  /**
   * Returns whether an operand is Analytical Function by traversing till next project rel
   * For ex, FilterRel e1 -> FilterRel e2 -> ProjectRel p -> TableScan ts
   * Here, we are traversing till ProjectRel p to check whether an operand of FilterRel e1
   * is Analytical function or not.
   */
  private static boolean isOperandAnalyticalInFollowingProject(RelNode rel,
      Integer rexOperandIndex) {
    if (rel instanceof Project) {
      return (((Project) rel).getProjects().size() - 1) >= rexOperandIndex
          && isAnalyticalRex(((Project) rel).getProjects().get(rexOperandIndex));
    } else if (rel.getInputs().size() > 0) {
      return isOperandAnalyticalInFollowingProject(rel.getInput(0), rexOperandIndex);
    }
    return false;
  }

  /** Returns whether an Analytical Function is present in filter condition. */
  protected boolean hasAnalyticalFunctionInFilter(Filter rel, RelNode input) {
    AnalyticalFunctionFinder finder = new AnalyticalFunctionFinder(true, input);
    try {
      rel.getCondition().accept(finder);
      return false;
    } catch (Util.FoundOne e) {
      return true;
    }
  }

  /** Returns whether an Analytical Function is present in joins.*/
  protected boolean hasAnalyticalFunctionInJoin(RelNode input) {
    if (input instanceof LogicalJoin && input.getInput(0) instanceof Project) {
      return isAnalyticalFunctionPresentInProjection((Project) input.getInput(0));
    }
    return false;
  }

  /* Returns whether any Analytical Function (RexOver) is present in projection.*/
  protected boolean isAnalyticalFunctionPresentInProjection(Project projectRel) {
    for (RexNode currentRex : projectRel.getProjects()) {
      if (isAnalyticalRex(currentRex)) {
        return true;
      }
    }
    return false;
  }

  protected static boolean isAnalyticalRex(RexNode rexNode) {
    if (rexNode instanceof RexOver) {
      return true;
    } else if (rexNode instanceof RexCall) {
      for (RexNode operand : ((RexCall) rexNode).getOperands()) {
        if (isAnalyticalRex(operand)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Walks over an expression and determines whether it is RexOver.
   */
  private static class AnalyticalFunctionFinder extends RexVisitorImpl<Void> {

    private RelNode inputRel;

    protected AnalyticalFunctionFinder(boolean deep, RelNode input) {
      super(deep);
      this.inputRel = input;
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      if (isOperandAnalyticalInFollowingProject(inputRel, index)) {
        throw Util.FoundOne.NULL;
      }
      return null;
    }

    @Override public Void visitOver(RexOver over) {
      throw Util.FoundOne.NULL;
    }

    @Override public Void visitCall(RexCall rexCall) {
      for (RexNode node : rexCall.getOperands()) {
        node.accept(this);
      }
      return null;
    }

  }

  /**
   * This function checks whether InnerJoinTrait having preserveInnerJoin status as true exist in RelTraitSet or not.
   */
  public static boolean preserveInnerJoin(RelTraitSet relTraitSet) {
    RelTrait relTrait = relTraitSet.getTrait(InnerJoinTraitDef.instance);
    return relTrait != null && relTrait instanceof InnerJoinTrait
        && ((InnerJoinTrait) relTrait).isPreserveInnerJoin();
  }
}

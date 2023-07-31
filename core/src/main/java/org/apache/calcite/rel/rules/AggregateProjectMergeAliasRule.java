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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Similar to $AggregateProjectMergeRule but also checks if project contains alias then don't merge.
 */

public class AggregateProjectMergeAliasRule extends AggregateProjectMergeRule {

  protected AggregateProjectMergeAliasRule(Config config) {
    super((AggregateProjectMergeRule.Config) config);
  }

  public static List<String> getFieldListName(RelNode relNode) {
    List<String> fieldNames = new ArrayList<String>();
    for (RelDataTypeField field: relNode.getRowType().getFieldList()) {
      fieldNames.add(field.getName());
    }
    return fieldNames;
  }

  public static @Nullable RelNode apply(RelOptRuleCall call, Aggregate aggregate,
      Project project) {
    // Check if project contains alias then don't merge.
    List<String> projectFieldList = getFieldListName(project);
    List<String> projectInputFieldList = getFieldListName(project.getInput());
    if (!projectInputFieldList.containsAll(projectFieldList)
        && !(aggregate.getAggCallList().size() > 0)) {
      return null;
    }
    return AggregateProjectMergeRule.apply(call, aggregate, project);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    AggregateProjectMergeRule.Config DEFAULT = EMPTY.as(AggregateProjectMergeRule.Config.class)
        .withOperandFor(Aggregate.class, Project.class);

    @Override default AggregateProjectMergeRule toRule() {
      return new AggregateProjectMergeAliasRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default AggregateProjectMergeRule.Config withOperandFor(
        Class<? extends Aggregate> aggregateClass,
        Class<? extends Project> projectClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass).oneInput(b1 ->
              b1.operand(projectClass).anyInputs())).as(AggregateProjectMergeRule.Config.class);
    }
  }
}

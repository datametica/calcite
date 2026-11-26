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
package org.apache.calcite.rel.rules.dm;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

/**
 * RemoveSingleRowCrossJoinRule removes unnecessary inner CROSS JOIN with a single-row LogicalValues.
 */
@Value.Enclosing
public class RemoveSingleRowCrossJoinRule
    extends RelRule<RemoveSingleRowCrossJoinRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /**
   * Creates the rule.
   */
  protected RemoveSingleRowCrossJoinRule(Config config) {
    super(config);
  }

  public void setExtension(RuleMatchExtension extension) {
    this.extension = extension;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    extension.execute(call);
  }

  /**
   * Rule configuration using Immutables-style.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    RemoveSingleRowCrossJoinRule.Config DEFAULT =
        ImmutableRemoveSingleRowCrossJoinRule.Config.of()
            .withOperandSupplier(b ->
                b.operand(LogicalProject.class)
                    .oneInput(r -> r.operand(LogicalJoin.class)
                        .inputs(
                            b1 -> b1.operand(Values.class)
                                .predicate(RemoveSingleRowCrossJoinRule::hasScalarValue)
                                .noInputs(),
                            b2 -> b2.operand(RelNode.class).anyInputs()))
            )
            .withDescription("RemoveSingleRowCrossJoinRule")
            .as(RemoveSingleRowCrossJoinRule.Config.class);

    @Override default RemoveSingleRowCrossJoinRule toRule() {
      return new RemoveSingleRowCrossJoinRule(this);
    }
  }

  public static boolean hasScalarValue(Values values) {
    return values.getTuples().size() == 1 && values.getTuples().get(0).size() == 1;
  }
}

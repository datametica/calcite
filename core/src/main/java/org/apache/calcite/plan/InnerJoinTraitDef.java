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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;

/**
 * InnerJoinTrait is used to identify if a given rel has a InnerJoinTraitDef.
 */
public class InnerJoinTraitDef extends RelTraitDef<InnerJoinTrait> {

  public static InnerJoinTraitDef instance = new InnerJoinTraitDef();

  @Override public Class<InnerJoinTrait> getTraitClass() {
    return InnerJoinTrait.class;
  }

  @Override public String getSimpleName() {
    return InnerJoinTrait.class.getSimpleName();
  }

  @Override public RelNode convert(RelOptPlanner planner, RelNode rel,
                                   InnerJoinTrait toTrait, boolean allowInfiniteCostConverters) {
    throw new UnsupportedOperationException("Method implementation"
        + " not supported for InnerJoinTrait");
  }

  @Override public boolean canConvert(RelOptPlanner planner, InnerJoinTrait fromTrait,
                                      InnerJoinTrait toTrait) {
    return false;
  }

  @Override public InnerJoinTrait getDefault() {
    throw new UnsupportedOperationException("Default implementation not "
        + "supported for InnerJoinTrait");
  }
}

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

/**
 * Records whether a join originated as a cross/comma join or as an explicit qualified join
 * in source SQL.
 */
public class SourceJoinFormTrait implements RelTrait {

  private final SourceJoinKind sourceJoinKind;

  public SourceJoinFormTrait(SourceJoinKind sourceJoinKind) {
    this.sourceJoinKind = sourceJoinKind;
  }

  public SourceJoinKind getSourceJoinKind() {
    return sourceJoinKind;
  }

  public boolean isCrossOrComma() {
    return sourceJoinKind == SourceJoinKind.CROSS_OR_COMMA;
  }

  @Override public RelTraitDef<SourceJoinFormTrait> getTraitDef() {
    return SourceJoinFormTraitDef.instance;
  }

  @Override public boolean satisfies(RelTrait trait) {
    return trait instanceof SourceJoinFormTrait
        && ((SourceJoinFormTrait) trait).sourceJoinKind == this.sourceJoinKind;
  }

  @Override public void register(RelOptPlanner planner) {
    throw new UnsupportedOperationException(
        "Registration not supported for SourceJoinFormTrait");
  }
}

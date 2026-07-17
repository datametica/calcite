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
 * Definition of the {@link CommentTrait} for use in the query planner.
 *
 *
 * <p>This trait definition allows comments to be attached to relational expressions (RelNodes)
 * as part of their trait set. It provides methods for trait class identification, default value,
 * and conversion logic (which is not supported for comments).
 * </p>
 *
 *
 *
 * <p>The {@code CommentTraitDef} is a singleton, accessible via {@link #INSTANCE}.
 * </p>
 */
public class CommentTraitDef extends RelTraitDef<CommentTrait> {
  /** Singleton instance of CommentTraitDef. */
  public static final CommentTraitDef INSTANCE = new CommentTraitDef();

  /**
   * Returns the class of the trait handled by this definition.
   *
   * @return the CommentTrait class
   */
  @Override public Class<CommentTrait> getTraitClass() {
    return CommentTrait.class;
  }

  /**
   * Returns a simple name for this trait definition.
   *
   * @return the string "comment"
   */
  @Override public String getSimpleName() {
    return "comment";
  }

  /**
   * Indicates whether this trait can be converted from one value to another.
   * For comments, conversion is not supported.
   *
   * @param planner the planner
   * @param fromTrait the source trait
   * @param toTrait the target trait
   * @return false always, as comment traits are not convertible
   */
  @Override public boolean canConvert(RelOptPlanner planner, CommentTrait fromTrait, CommentTrait toTrait) {
    return false;
  }

  /**
   * Returns the default value for this trait, which is an empty comment map.
   *
   * @return a CommentTrait with an empty map
   */
  @Override public CommentTrait getDefault() {
    return new CommentTrait(java.util.Collections.emptyMap(), java.util.Collections.emptySet());
  }

  /**
   * Converts a relational expression to use the given trait. Not supported for comments.
   *
   * @param planner the planner
   * @param rel the relational expression
   * @param toTrait the target trait
   * @param allowInfiniteCostConverters whether infinite cost converters are allowed
   * @return null always, as conversion is not supported
   */
  @Override public RelNode convert(RelOptPlanner planner, RelNode rel, CommentTrait toTrait,
      boolean allowInfiniteCostConverters) {
    return null;
  }
}

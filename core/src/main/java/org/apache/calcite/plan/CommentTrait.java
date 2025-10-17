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

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Comment;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A trait for attaching comments to relational expressions (RelNodes) in the query plan.
 *
 *
 * <p>This trait holds a mapping from RexNode expressions to their associated comments, allowing
 * comments to be preserved and propagated through the planning process. It implements the
 * {@link RelTrait} interface so it can be attached to a RelNode's trait set.
 * </p>
 */
public class CommentTrait implements RelTrait {
  /**
   * Map of RexNode expressions to their associated comments.
   */
  private final Map<RexNode, Set<Comment>> rexNodeCommentListMap;
  private final Set<Comment> commentSet = new HashSet<>();

  /**
   * Creates a CommentTrait with the given comments map.
   *
   * @param rexNodeCommentListMap Map from RexNode to comment string
   */
  public CommentTrait(Map<RexNode, Set<Comment>> rexNodeCommentListMap, @Nullable Set<Comment> commentList) {
    this.rexNodeCommentListMap = rexNodeCommentListMap;
    if (commentList != null) {
      this.commentSet.addAll(commentList);
    }
  }

  /**
   * Returns the map of comments associated with this trait.
   *
   * @return Map from RexNode to comment string
   */
  public Map<RexNode, Set<Comment>> getCommentsMap() {
    return this.rexNodeCommentListMap;
  }

  public Set<Comment> getCommentSet() {
    return commentSet;
  }

  @Override public CommentTraitDef getTraitDef() {
    return CommentTraitDef.INSTANCE;
  }

  @Override public boolean satisfies(RelTrait relTrait) {
    return relTrait == this;
  }

  @Override public String toString() {
    return "CommentTrait";
  }

  @Override public void register(RelOptPlanner planner) {
  }
}

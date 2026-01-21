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
package org.apache.calcite.rex;

import org.apache.calcite.util.Comment;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstract base class for nodes that can hold comments in the Rex tree.
 */
public abstract class CommentNode {

  private final Set<Comment> comment;

  public Set<Comment> getComment() {
    return comment;
  }

  CommentNode() {
    this.comment = new HashSet<>();
  }

  CommentNode(Set<Comment> comment) {
    this.comment = comment;
  }

  public abstract RexNode copy(Set<Comment> comments);
}

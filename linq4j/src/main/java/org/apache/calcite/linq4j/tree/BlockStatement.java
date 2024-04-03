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
package org.apache.calcite.linq4j.tree;

import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a block that contains a sequence of expressions where variables
 * can be defined.
 */
public class BlockStatement extends Statement {
  public final List<Node> nodes;
  /** Cached hash code for the expression. */
  private int hash;

  BlockStatement(List<Node> nodes, Type type) {
    super(ExpressionType.Block, type);
    assert nodes != null : "nodes should not be null";
    this.nodes = nodes;
    assert distinctVariables(true);
  }

  private boolean distinctVariables(
      @UnderInitialization(BlockStatement.class) BlockStatement this,
      boolean fail
  ) {
    Set<String> names = new HashSet<>();
    for (Node node : nodes) {
      if (node instanceof DeclarationStatement) {
        String name = ((DeclarationStatement) node).parameter.name;
        if (!names.add(name)) {
          assert !fail : "duplicate variable " + name;
          return false;
        }
      }
    }
    return true;
  }

  @Override public BlockStatement accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    List<Node> newnodes = Expressions.acceptNodes(nodes,
        shuttle);
    return shuttle.visit(this, newnodes);
  }

  @Override public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override void accept0(ExpressionWriter writer) {
    if (nodes.isEmpty()) {
      writer.append("{}");
      return;
    }
    writer.begin("{\n");
    for (Node node : nodes) {
      node.accept(writer);
    }
    writer.end("}\n");
  }

  @Override public @Nullable Object evaluate(Evaluator evaluator) {
    Object o = null;
    for (Node node : nodes) {
      if (Statement.class.isInstance(node)) {
        o = ((Statement) node).evaluate(evaluator);
      }
    }
    return o;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    BlockStatement that = (BlockStatement) o;

    if (!nodes.equals(that.nodes)) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    int result = hash;
    if (result == 0) {
      result = Objects.hash(nodeType, type, nodes);
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}

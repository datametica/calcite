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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalQualifier;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RexInterval extends RexNode {
  public final RexNode node;
  public final SqlIntervalQualifier sqlIntervalQualifier;
  public final RelDataType type;

  RexInterval(RexNode node, SqlIntervalQualifier sqlIntervalQualifier) {
    this.node = requireNonNull(node, "node");
    this.sqlIntervalQualifier = requireNonNull(sqlIntervalQualifier, "sqlIntervalQualifier");
    this.type = requireNonNull(node.getType(), "type");
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitRexInterval(this);
  }

  @Override public RelDataType getType() {
    return type;
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitRexInterval(this, arg);
  }

  @Override
  public String toString() {
    return "RexInterval{" +
        "node=" + node +
        ", sqlIntervalQualifier=" + sqlIntervalQualifier +
        ", type=" + type +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RexInterval)) return false;
    RexInterval that = (RexInterval) o;
    return Objects.equals(node, that.node) &&
        Objects.equals(sqlIntervalQualifier, that.sqlIntervalQualifier) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(node, sqlIntervalQualifier, type);
  }
}

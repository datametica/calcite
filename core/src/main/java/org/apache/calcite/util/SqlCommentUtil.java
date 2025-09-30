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
package org.apache.calcite.util;

import org.apache.calcite.plan.CommentTrait;
import org.apache.calcite.plan.CommentTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.*;

public class SqlCommentUtil {
  public static void addCommentToSqlNode(SqlNode sqlNode, RelNode relNode) {
    CommentTrait commentTrait = relNode.getTraitSet().getTrait(CommentTraitDef.INSTANCE);
    if (commentTrait != null) {
      Set<Comment> comments = commentTrait.getCommentSet();
//      comments.addAll(commentTrait.getCommentsMap().values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
      sqlNode.setCommentList(comments);
    }
  }


  public static Set<Comment> getCommentsInMap(RelNode relNode, RexNode rex) {
    CommentTrait commentTrait = relNode.getTraitSet().getTrait(CommentTraitDef.INSTANCE);
    if (commentTrait == null) {
      return new HashSet<>();
    }
    Map<RexNode, Set<Comment>> rexNodeCommentListMap = commentTrait.getCommentsMap();
    Set<Comment> result = rexNodeCommentListMap.get(rex);
    if (result != null) {
      return result;
    }
    result = new HashSet<>();
    Set<Comment> finalResult = result;
    rexNodeCommentListMap.entrySet().stream()
        .filter(entry -> entry.getKey() instanceof RexCall)
        .forEach(entry -> {
      if (((RexCall) entry.getKey()).operands.contains(rex)) {
        finalResult.addAll(entry.getValue());
      }
    });
    return result;
  }

  public static Set<Comment> getCommentsInMap(RelNode relNode, RelFieldCollation field) {
    CommentTrait commentTrait = relNode.getTraitSet().getTrait(CommentTraitDef.INSTANCE);
    if (commentTrait == null) {
      return Collections.emptySet();
    }

    Map<RexNode, Set<Comment>> rexNodeCommentListMap = commentTrait.getCommentsMap();
    Set<Comment> result = new HashSet<>();

    // Step 1: find RexInputRef for this field index
    int targetIndex = field.getFieldIndex();
    RexInputRef targetRef = null;
    for (RexNode node : rexNodeCommentListMap.keySet()) {
      if (node instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) node;
        if (inputRef.getIndex() == targetIndex) {
          targetRef = inputRef;
          break;
        }
      }
    }

    // Step 2: collect comments directly mapped to RexInputRef
    if (targetRef != null) {
      Set<Comment> directComments = rexNodeCommentListMap.get(targetRef);
      if (directComments != null) {
        result.addAll(directComments);
      }
    }

    // Step 3: also check RexCall operands
    for (Map.Entry<RexNode, Set<Comment>> entry : rexNodeCommentListMap.entrySet()) {
      RexNode key = entry.getKey();
      if (key instanceof RexCall) {
        RexCall call = (RexCall) key;
        for (RexNode operand : call.operands) {
          if (operand instanceof RexInputRef) {
            if (((RexInputRef) operand).getIndex() == targetIndex) {
              result.addAll(entry.getValue());
              break; // no need to check more operands of this call
            }
          }
        }
      }
    }

    return result;
  }

}

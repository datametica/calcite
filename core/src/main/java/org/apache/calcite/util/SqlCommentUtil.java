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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
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
    rexNodeCommentListMap.entrySet().stream().filter(entry -> entry.getKey() instanceof RexCall).forEach(entry -> {
      if (((RexCall) entry.getKey()).operands.contains(rex)) {
        finalResult.addAll(entry.getValue());
      }
    });
    return result;
  }
}

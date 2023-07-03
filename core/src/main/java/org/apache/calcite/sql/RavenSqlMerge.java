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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Handles merge statement.
 */
public class RavenSqlMerge extends SqlMerge {
  SqlDelete deleteCall;

  public RavenSqlMerge(SqlParserPos pos, SqlNode targetTable, SqlNode condition, SqlNode source,
      @Nullable SqlUpdate updateCall, @Nullable SqlDelete deleteCall,
      @Nullable SqlInsert insertCall, @Nullable SqlSelect sourceSelect,
      @Nullable SqlIdentifier alias) {
    super(pos, targetTable, condition, source, updateCall, insertCall, sourceSelect, alias);
    this.deleteCall = deleteCall;
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      assert operand instanceof SqlIdentifier;
      targetTable = operand;
      break;
    case 1:
      condition = operand;
      break;
    case 2:
      source = operand;
      break;
    case 3:
      updateCall = (@Nullable SqlUpdate) operand;
      break;
    case 4:
      deleteCall = (@Nullable SqlDelete) operand;
      break;
    case 5:
      insertCall = (@Nullable SqlInsert) operand;
      break;
    case 6:
      sourceSelect = (@Nullable SqlSelect) operand;
      break;
    case 7:
      alias = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MERGE INTO",
        "");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    targetTable.unparse(writer, opLeft, opRight);
    SqlIdentifier alias = this.alias;
    if (alias != null) {
      writer.keyword("AS");
      alias.unparse(writer, opLeft, opRight);
    }

    writer.newlineAndIndent();
    writer.keyword("USING");
    source.unparse(writer, opLeft, opRight);

    writer.newlineAndIndent();
    writer.keyword("ON");
    condition.unparse(writer, opLeft, opRight);

    SqlUpdate updateCall = this.updateCall;
    if (updateCall != null) {
      writer.newlineAndIndent();
      writer.keyword("WHEN MATCHED");
      if (updateCall.condition != null) {
        writer.keyword("AND");
        updateCall.condition.unparse(writer, leftPrec, rightPrec);
      }
      writer.keyword("THEN");
      writer.keyword("UPDATE");
      final SqlWriter.Frame setFrame = writer.startList(SqlWriter.FrameTypeEnum.UPDATE_SET_LIST,
          "SET", "");

      for (Pair<SqlNode, SqlNode> pair : Pair.zip(updateCall.targetColumnList,
          updateCall.sourceExpressionList)) {
        writer.sep(",");
        SqlIdentifier id = (SqlIdentifier) pair.left;
        id.unparse(writer, opLeft, opRight);
        writer.keyword("=");
        SqlNode sourceExp = pair.right;
        sourceExp.unparse(writer, opLeft, opRight);
      }
      writer.endList(setFrame);

      if (deleteCall != null) {
        writer.newlineAndIndent();
        writer.keyword("WHEN MATCHED");
        if (deleteCall.condition != null) {
          writer.keyword("AND");
          deleteCall.condition.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("THEN");
        writer.newlineAndIndent();
        writer.keyword("DELETE");
      }
    }

    SqlInsert insertCall = this.insertCall;
    if (insertCall != null) {
      writer.newlineAndIndent();
      writer.keyword("WHEN NOT MATCHED");
      if (insertCall.condition != null) {
        writer.keyword("AND");
        insertCall.condition.unparse(writer, leftPrec, rightPrec);
      }
      writer.keyword("THEN INSERT");
      SqlNodeList targetColumnList = insertCall.getTargetColumnList();
      if (targetColumnList != null) {
        targetColumnList.unparse(writer, opLeft, opRight);
      }
      insertCall.getSource().unparse(writer, opLeft, opRight);

      writer.endList(frame);
    }
  }

}

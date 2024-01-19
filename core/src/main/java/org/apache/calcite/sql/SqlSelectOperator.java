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

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * An operator describing a query. (Not a query itself.)
 *
 * <p>Operands are:
 *
 * <ul>
 * <li>0: distinct ({@link SqlLiteral})</li>
 * <li>1: selectClause ({@link SqlNodeList})</li>
 * <li>2: fromClause ({@link SqlCall} to "join" operator)</li>
 * <li>3: whereClause ({@link SqlNode})</li>
 * <li>4: havingClause ({@link SqlNode})</li>
 * <li>5: groupClause ({@link SqlNode})</li>
 * <li>6: windowClause ({@link SqlNodeList})</li>
 * <li>7: orderClause ({@link SqlNode})</li>
 * </ul>
 */
public class SqlSelectOperator extends SqlOperator {
  public static final SqlSelectOperator INSTANCE =
      new SqlSelectOperator();

  //~ Constructors -----------------------------------------------------------

  private SqlSelectOperator() {
    super("SELECT", SqlKind.SELECT, 2, true, ReturnTypes.SCOPE, null, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  @Override public SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    assert functionQualifier == null;
    return new SqlSelect(pos,
        (SqlNodeList) operands[0],
        requireNonNull((SqlNodeList) operands[1], "selectList"),
        operands[2],
        operands[3],
        (SqlNodeList) operands[4],
        operands[5],
        (SqlNodeList) operands[6],
        operands[7],
        (SqlNodeList) operands[8],
        operands[9],
        operands[10],
        (SqlNodeList) operands[11]);
  }

  /**
   * Creates a call to the <code>SELECT</code> operator.
   *
   * @deprecated Use {@link #createCall(SqlLiteral, SqlParserPos, SqlNode...)}.
   */
  @Deprecated // to be removed before 2.0
  public SqlSelect createCall(
      SqlNodeList keywordList,
      SqlNodeList selectList,
      SqlNode fromClause,
      SqlNode whereClause,
      SqlNodeList groupBy,
      SqlNode having,
      SqlNodeList windowDecls,
      SqlNode qualify,
      SqlNodeList orderBy,
      SqlNode offset,
      SqlNode fetch,
      SqlNodeList hints,
      SqlParserPos pos) {
    return new SqlSelect(
        pos,
        keywordList,
        selectList,
        fromClause,
        whereClause,
        groupBy,
        having,
        windowDecls,
        qualify,
        orderBy,
        offset,
        fetch,
        hints);
  }

  @Override public <R> void acceptCall(
      SqlVisitor<R> visitor,
      SqlCall call,
      boolean onlyExpressions,
      SqlBasicVisitor.ArgHandler<R> argHandler) {
    if (!onlyExpressions) {
      // None of the arguments to the SELECT operator are expressions.
      super.acceptCall(visitor, call, onlyExpressions, argHandler);
    }
  }

  @SuppressWarnings("deprecation")
  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    SqlSelect select = (SqlSelect) call;
    final SqlWriter.Frame selectFrame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep("SELECT");

    if (select.hasHints()) {
      writer.sep("/*+");
      castNonNull(select.hints).unparse(writer, 0, 0);
      writer.print("*/");
      writer.newlineAndIndent();
    }

    for (int i = 0; i < select.keywordList.size(); i++) {
      final SqlNode keyword = select.keywordList.get(i);
      keyword.unparse(writer, 0, 0);
    }
    writer.topN(select.fetch, select.offset);
    final SqlNodeList selectClause = select.selectList;
    writer.list(SqlWriter.FrameTypeEnum.SELECT_LIST, SqlWriter.COMMA,
        selectClause);

    if (select.from != null) {
      // Calcite SQL requires FROM but MySQL does not.
      writer.sep("FROM");

      // for FROM clause, use precedence just below join operator to make
      // sure that an un-joined nested select will be properly
      // parenthesized
      final SqlWriter.Frame fromFrame =
          writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST);
      select.from.unparse(
          writer,
          SqlJoin.COMMA_OPERATOR.getLeftPrec() - 1,
          SqlJoin.COMMA_OPERATOR.getRightPrec() - 1);
      writer.endList(fromFrame);
    }

    SqlNode where = select.where;
    if (where != null) {
      writer.sep("WHERE");

      if (!writer.isAlwaysUseParentheses()) {
        SqlNode node = where;

        // decide whether to split on ORs or ANDs
        SqlBinaryOperator whereSep = SqlStdOperatorTable.AND;
        if ((node instanceof SqlCall)
            && node.getKind() == SqlKind.OR) {
          whereSep = SqlStdOperatorTable.OR;
        }

        // unroll whereClause
        final List<SqlNode> list = new ArrayList<>(0);
        while (node.getKind() == whereSep.kind) {
          assert node instanceof SqlCall;
          final SqlCall call1 = (SqlCall) node;
          list.add(0, call1.operand(1));
          node = call1.operand(0);
        }
        list.add(0, node);

        // unparse in a WHERE_LIST frame
        writer.list(SqlWriter.FrameTypeEnum.WHERE_LIST, whereSep,
            new SqlNodeList(list, where.getParserPosition()));
      } else {
        where.unparse(writer, 0, 0);
      }
    }
    if (select.groupBy != null) {
      writer.sep("GROUP BY");
      if (select.groupBy.getList().isEmpty()) {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
        writer.endList(frame);
      } else {
        if (writer.getDialect().getConformance().isGroupByOrdinal()) {
          final SqlWriter.Frame groupFrame =
              writer.startList(SqlWriter.FrameTypeEnum.GROUP_BY_LIST);
          List<SqlNode> visitedLiteralNodeList = new ArrayList<>();
          for (SqlNode groupKey : select.groupBy.getList()) {
            if (!groupKey.toString().equalsIgnoreCase("NULL")) {
              if (groupKey.getKind() == SqlKind.LITERAL
                  || groupKey.getKind() == SqlKind.DYNAMIC_PARAM
                  || groupKey.getKind() == SqlKind.MINUS_PREFIX) {
                select.selectList.getList().
                    forEach(new Consumer<SqlNode>() {
                      @Override public void accept(SqlNode selectSqlNode) {
                        SqlNode literalNode = selectSqlNode;
                        if (literalNode.getKind() == SqlKind.AS) {
                          literalNode = ((SqlBasicCall) selectSqlNode).getOperandList().get(0);
                          if (SqlKind.CAST == literalNode.getKind()) {
                            literalNode = ((SqlBasicCall) literalNode).getOperandList().get(0);
                          }
                        }
                        if (SqlKind.CAST == literalNode.getKind()) {
                          literalNode = ((SqlBasicCall) literalNode).getOperandList().get(0);
                        }
                        if (literalNode == groupKey
                            && !visitedLiteralNodeList.contains(literalNode)) {
                          writer.sep(",");
                          String ordinal = String.valueOf(select.selectList
                                  .getList().indexOf(selectSqlNode) + 1);
                          SqlLiteral.createExactNumeric(ordinal,
                              SqlParserPos.ZERO).unparse(writer, 2, 3);
                          visitedLiteralNodeList.add(literalNode);
                        }
                      }
                    });
              } else {
                writer.sep(",");
                groupKey.unparse(writer, 2, 3);
              }
            }
          }
          writer.endList(groupFrame);
        } else {
          writer.list(SqlWriter.FrameTypeEnum.GROUP_BY_LIST, SqlWriter.COMMA, select.groupBy);
        }
      }
    }
    if (select.having != null) {
      writer.sep("HAVING");
      select.having.unparse(writer, 0, 0);
    }
    if (select.windowDecls.size() > 0) {
      writer.sep("WINDOW");
      writer.list(SqlWriter.FrameTypeEnum.WINDOW_DECL_LIST, SqlWriter.COMMA,
          select.windowDecls);
    }
    if (select.qualify != null) {
      writer.sep("QUALIFY");
      select.qualify.unparse(writer, 0, 0);
    }
    if (select.orderBy != null && select.orderBy.size() > 0) {
      writer.sep("ORDER BY");
      writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
          select.orderBy);
    }
    writer.fetchOffset(select.fetch, select.offset);
    writer.endList(selectFrame);
  }

  @Override public boolean argumentMustBeScalar(int ordinal) {
    return ordinal == SqlSelect.WHERE_OPERAND;
  }
}

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
    final SqlNodeList selectClause = select.selectList != null
            ? select.selectList
            : SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO));
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
      SqlNodeList groupBy =
              select.groupBy.size() == 0 ? SqlNodeList.SINGLETON_EMPTY
                      : select.groupBy;
      // if the DISTINCT keyword of GROUP BY is present it can be the only item
      if (groupBy.size() == 1 && groupBy.get(0) != null
              && groupBy.get(0).getKind() == SqlKind.GROUP_BY_DISTINCT) {
        writer.sep("GROUP BY DISTINCT");
        List<SqlNode> operandList = ((SqlCall) groupBy.get(0)).getOperandList();
        groupBy = new SqlNodeList(operandList, groupBy.getParserPosition());
      } else {
        writer.sep("GROUP BY");
      }
      if (select.groupBy.getList().isEmpty()) {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
        writer.endList(frame);
      } else {
        if (writer.getDialect().getConformance().isGroupByOrdinal()) {
          final SqlWriter.Frame groupFrame =
              writer.startList(SqlWriter.FrameTypeEnum.GROUP_BY_LIST);
          List<SqlNode> visitedSqlNodes = new ArrayList<>();
          for (SqlNode groupKey : select.groupBy.getList()) {
            if (!groupKey.toString().equalsIgnoreCase("NULL")) {
              if (shouldProcessGroupKey(groupKey)) {
                select.selectList.getList().
                    forEach(new Consumer<SqlNode>() {
                      @Override public void accept(SqlNode selectSqlNode) {
                        SqlNode sqlNode = unwrapSqlNode(selectSqlNode);
                        if (isLiteralOrDynamicOrMinusUsedInGroupBy(selectSqlNode, sqlNode, groupKey, visitedSqlNodes)
                            || isCastOperandUsedInGroupBy(selectSqlNode, sqlNode, groupKey, visitedSqlNodes)) {
                              unparseGroupByOrdinal(selectSqlNode, sqlNode, writer, select, visitedSqlNodes);
                        }
                      }
                    });
                if (isGroupByKeyNotVisited(groupKey, visitedSqlNodes)) {
                  unparseGroupKey(writer, groupKey);
                  visitedSqlNodes.add(groupKey);
                }
              } else {
                unparseGroupKey(writer, groupKey);
              }
            }
          }
          writer.endList(groupFrame);
        } else {
          writer.list(SqlWriter.FrameTypeEnum.GROUP_BY_LIST, SqlWriter.COMMA, groupBy);
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

  private static void unparseGroupKey(SqlWriter writer, SqlNode groupKey) {
    writer.sep(",");
    groupKey.unparse(writer, 2, 3);
  }

  private static SqlNode unwrapSqlNode(SqlNode selectSqlNode) {
    return extractCastOperand(selectSqlNode.getKind() == SqlKind.AS
        ? ((SqlBasicCall) selectSqlNode).getOperandList().get(0) : selectSqlNode);
  }

  private static SqlNode extractCastOperand(SqlNode sqlNode) {
    return SqlKind.CAST == sqlNode.getKind() ? ((SqlBasicCall) sqlNode).getOperandList().get(0) : sqlNode;
  }

  private static boolean isGroupByKeyNotVisited(SqlNode groupKey, List<SqlNode> visitedSqlNodes) {
    return !visitedSqlNodes.contains(groupKey) && visitedSqlNodes.stream()
        .noneMatch(sqlNode -> isCastOnGroupKey(sqlNode, groupKey));
  }

  private static void unparseGroupByOrdinal(SqlNode selectSqlNode, SqlNode sqlNode, SqlWriter writer,
      SqlSelect select, List<SqlNode> visitedSqlNodes) {
    writer.sep(",");
    String ordinal =
        String.valueOf(select.selectList.getList().
                indexOf(selectSqlNode) + 1);
    SqlLiteral.createExactNumeric(ordinal,
        SqlParserPos.ZERO).unparse(writer, 2, 3);
    visitedSqlNodes.add(sqlNode);
  }

  private static boolean isCastOperandUsedInGroupBy(SqlNode selectSqlNode, SqlNode sqlNode,
      SqlNode groupKey, List<SqlNode> visitedSqlNodes) {
    return selectSqlNode.getKind() == SqlKind.CAST
        && (sqlNode.equals(groupKey) || isCastOnGroupKey(sqlNode, groupKey))
        && !visitedSqlNodes.contains(sqlNode);
  }

  private boolean isLiteralOrDynamicOrMinusUsedInGroupBy(SqlNode selectSqlNode, SqlNode sqlNode,
      SqlNode groupKey, List<SqlNode> visitedSqlNodes) {
    return sqlNode.equals(groupKey)
        && !visitedSqlNodes.contains(sqlNode) && (!(selectSqlNode instanceof SqlBasicCall)
        || (isLiteralOrDynamicOrMinusPrefix(groupKey) && selectSqlNode.getKind() == SqlKind.AS));
  }

  private boolean shouldProcessGroupKey(SqlNode groupKey) {
    return isLiteralOrDynamicOrMinusPrefix(groupKey) || groupKey instanceof SqlBasicCall;
  }

  private boolean isLiteralOrDynamicOrMinusPrefix(SqlNode groupKey) {
    return groupKey.getKind() == SqlKind.LITERAL
        || groupKey.getKind() == SqlKind.DYNAMIC_PARAM
        || groupKey.getKind() == SqlKind.MINUS_PREFIX;
  }

  private static boolean isCastOnGroupKey(SqlNode sqlNode, SqlNode groupKey) {
    return sqlNode.getKind() == SqlKind.CAST
        && ((SqlBasicCall) sqlNode).getOperandList().get(0) == groupKey;
  }

  @Override public boolean argumentMustBeScalar(int ordinal) {
    return ordinal == SqlSelect.WHERE_OPERAND;
  }
}

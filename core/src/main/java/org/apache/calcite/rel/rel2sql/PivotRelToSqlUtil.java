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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.plan.PivotRelTrait;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;

/**
 * Class to identify Rel structure which is of UNPIVOT Type.
 */

public class PivotRelToSqlUtil {
  SqlParserPos pos;
  String pivotTableAlias = "";

  PivotRelToSqlUtil(SqlParserPos pos) {
    this.pos = pos;
  }
  /**
   *  Builds SqlPivotNode for Aggregate RelNode.
   *
   * @param e The aggregate node with pivot relTrait flag
   * @param builder The SQL builder
   * @param selectColumnList  selectNodeList from Project node
   * @return  Result with sqlPivotNode wrap in it.
   */
  public SqlNode buildSqlPivotNode(
      Aggregate e, SqlImplementor.Builder builder, List<SqlNode> selectColumnList,
      List<SqlNode> aggregateColList) {
    //create query parameter
    Optional<RelTrait> pivotRelTrait =
        e.getTraitSet().stream().filter(relTrait -> relTrait instanceof PivotRelTrait).findAny();
    boolean checksubquery = ((PivotRelTrait) pivotRelTrait.get()).getsubqueryPresentFlag();
    String alias = ((PivotRelTrait) pivotRelTrait.get()).getPivotAlias();
    if (!alias.equals("false")) {
      pivotTableAlias = alias;
    }
    SqlNode query;
    if (checksubquery) {
      query = builder.select;
    } else {
      query = builder.select.getFrom();
    }

    //create aggList parameter
    SqlNodeList axisNodeList = getAxisNodeList(selectColumnList, checksubquery);

    //create axisList parameter
    SqlNodeList aggList = getAxisSqlNodes(e);

    //create inValues List parameter
    SqlNodeList inColumnList = getInValueNodes(selectColumnList, aggregateColList, axisNodeList);

    //create Pivot Node

    return wrapSqlPivotInSqlSelectSqlNode(
        builder, query, aggList, axisNodeList, inColumnList);
  }

  private SqlNode wrapSqlPivotInSqlSelectSqlNode(
      SqlImplementor.Builder builder, SqlNode query, SqlNodeList aggList,
      SqlNodeList axisNodeList, SqlNodeList inColumnList) {
    SqlPivot sqlPivot = new SqlPivot(pos, query, aggList, axisNodeList, inColumnList);
    SqlNode sqlTableAlias = sqlPivot;
    if (pivotTableAlias.length() > 0) {
      sqlTableAlias =
          SqlStdOperatorTable.AS.createCall(pos, sqlPivot,
              new SqlIdentifier(pivotTableAlias, pos));
    }
    SqlNode select =
        new SqlSelect(SqlParserPos.ZERO, null, null, sqlTableAlias,
            builder.select.getWhere(), null,
            builder.select.getHaving(), null, builder.select.getOrderList(),
            null, null, SqlNodeList.EMPTY);
    return select;
  }

  private SqlNodeList getInValueNodes(List<SqlNode> selectList, List<SqlNode> aggregateColList,
      SqlNodeList axisNode) {
    SqlNodeList inColumnList = new SqlNodeList(pos);

    if (aggregateColList.size() == 0) {
      selectList.stream()
          .filter(x -> !(x instanceof SqlIdentifier))
          .map(node -> {
            // Extract the specific node as per your expression
            SqlNode secondOperand = ((SqlBasicCall)
                ((SqlCase)
                    ((SqlBasicCall)
                        ((SqlBasicCall) node)
                            .getOperandList().get(0))
                        .operand(0))
                    .getWhenOperands().get(0))
                .operand(1);

            if (secondOperand instanceof SqlCharStringLiteral) {
              return SqlLiteral.createCharString(
                  ((NlsString) ((SqlCharStringLiteral) secondOperand)
                  .getValue()).getValue(), pos);
            }
            if (secondOperand instanceof SqlBasicCall && ((SqlBasicCall) secondOperand)
                .getOperator().kind == SqlKind.AS) {
              return secondOperand;
            }

            // Extract the type name from the first operand
            SqlTypeName sqlTypeName =
                ((SqlCharStringLiteral)
                    ((SqlBasicCall)
                        ((SqlCase)
                            ((SqlBasicCall)
                                ((SqlBasicCall) node)
                                    .getOperandList().get(0))
                                .operand(0))
                            .getWhenOperands().get(0))
                        .operand(0))
                    .getTypeName();

            SqlNode sqlType =
                new SqlDataTypeSpec(new SqlBasicTypeNameSpec(sqlTypeName, SqlParserPos.ZERO),
                    SqlParserPos.ZERO);

            // Create a CAST operation using the extracted SQL type name
            SqlNode castedNode = CAST.createCall(SqlParserPos.ZERO, secondOperand, sqlType);

            return castedNode;
          })
          .forEach(extractedNode -> inColumnList.add(extractedNode));
    }
    SqlNode axis = axisNode.get(0);
    for (int i = 0; i < aggregateColList.size(); i++) {
      SqlNode secondOperand = aggregateColList.get(i);
      if (((SqlCharStringLiteral) axis).getTypeName() != ((SqlNumericLiteral) secondOperand)
          .getTypeName()) {
        SqlNode sqlType =
            new SqlDataTypeSpec(
                new SqlBasicTypeNameSpec(((SqlLiteral) axis).getTypeName(),
                SqlParserPos.ZERO),
                SqlParserPos.ZERO);
        inColumnList.add(CAST.createCall(SqlParserPos.ZERO, secondOperand, sqlType));
      }
    }

    return inColumnList;
  }

  private SqlNodeList getAxisSqlNodes(Aggregate e) {
    Set<SqlNode> aggArgList = new HashSet<>();
    Set<String> columnName = new HashSet<>();
    for (int i = 0; i < e.getAggCallList().size(); i++) {
      columnName.add(
          e.getInput().getRowType().getFieldList().get(
                  e.getAggCallList().get(i).getArgList().get(0))
              .getKey());
    }
    SqlNode tempNode = new SqlIdentifier(new ArrayList<>(columnName).get(0), pos);
    SqlNode aggFunctionNode =
        e.getAggCallList().get(0).getAggregation().createCall(pos, tempNode);
    aggArgList.add(aggFunctionNode);
    SqlNodeList axesNodeList = new SqlNodeList(aggArgList, pos);
    return axesNodeList;
  }

  private SqlNodeList getAxisNodeList(List<SqlNode> selectColumnList, boolean checksubquery) {

    final Set<SqlNode> selectList = new HashSet<>();

    SqlBasicCall pivotColumnAggregation =
        (SqlBasicCall) selectColumnList.get(selectColumnList.size() - 1);

    if (!checksubquery) {
      SqlCase pivotColumnAggregationCaseCall =
          (SqlCase) (
              (SqlBasicCall) ((SqlBasicCall) selectColumnList
              .get(selectColumnList.size() - 1))
              .getOperandList().get(0)).getOperandList().get(0);
      SqlBasicCall caseConditionCall =
          (SqlBasicCall) pivotColumnAggregationCaseCall.getWhenOperands().get(0);
      SqlCharStringLiteral aggregateCol = caseConditionCall.operand(0);
      selectList.add(aggregateCol);
      return new SqlNodeList(selectList, pos);
    }
    SqlCharStringLiteral aggregateCol =
        ((SqlBasicCall) pivotColumnAggregation.getOperandList().get(0)).operand(0);
    selectList.add(aggregateCol);
    return new SqlNodeList(selectList, pos);

  }
}

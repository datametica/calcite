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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.rel.rel2sql.SqlImplementor.POS;

/**
 * If the given project rel has flattened fields because of * in the source query
 * e.g : select empno, * from emp ;
 *
 *
 * <p>REL structure :
 * LogicalProject(EMPNO=[$0], EMPNO0=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],
 * SAL=[$5], COMM=[$6], DEPTNO=[$7], SLACKER=[$8])
 * LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *
 *
 * <p>In a given project rel, identify the sublist from the projection list which has indices/field
 * ordinals in the same sequence as of input rel.
 * Once identified, replace this sublist back with SQlIdentifier.STAR to achieve below as sqlNode
 * SqlNode : select empno, * from emp;
 */
public class StarProjectionUtils {

  SqlImplementor sqlImplementor;
  public SqlDialect dialect;
  StarProjectionUtils(SqlImplementor sqlImplementor) {
    this.sqlImplementor = sqlImplementor;
  }

  public static Map<Integer, Integer> identifyStarProjectionSublistIndices(List<RexNode> projects,
      RelNode input, Project e) {
    Map<Integer, Integer> subListBeginEnd = new HashMap<>();
    Map<Integer, Integer> emptyMap = new HashMap<>();
    if (canCombineProjects(projects, input)) {
      for (int start = 0; start < projects.size(); start++) {
        for (int end = start + 1; end < projects.size(); end++) {
          List<RexNode> sublist = projects.subList(start, end + 1);
          //if given sublist has ordinals same as the underlying input
          if (sublist.stream().allMatch(p -> p instanceof RexInputRef)
              && isSubListOrdinalsSameAsInput(input, sublist)) {
            subListBeginEnd.put(start, end);
            start = end;
            break;
          }
        }
      }
    }
    return isFieldNameSame(e, subListBeginEnd) ? subListBeginEnd : emptyMap;
  }

  private static boolean isSubListOrdinalsSameAsInput(RelNode input, List<RexNode> sublist) {
    return sublist.stream().map(p -> ((RexInputRef) p).getIndex())
        .collect(Collectors.toList()).equals(
            IntStream.range(0, input.getRowType().getFieldCount()).boxed().
                collect(Collectors.toList()));
  }

  private static boolean isFieldNameSame(Project e, Map<Integer, Integer> subListBeginEnd) {
    List<String> projectedColumnNames = e.getRowType().getFieldNames();
    RelNode projectRelNode = e;
    // This is to iterate till TableScan or Join rel is obtained
    while (!projectRelNode.getInputs().isEmpty()) {
      projectRelNode = projectRelNode.getInput(0);
    }
    List<String> actualColumnNames = projectRelNode.getRowType().getFieldNames();
    return columnNamesMatchesActualFieldNames(projectedColumnNames, actualColumnNames,
        subListBeginEnd);
  }

  /**
   * This method evaluates whether the projected column names
   * matches the actual column names in a table.
   *
   * Eg:
   * query -> select warehouse_class_id as id, description as description
   *           from warehouse_class
   * Rel - LogicalProject(id=[$0], description=[$1])
   *          JdbcTableScan(table=[[foodmart, warehouse_class]])
   * Actual field names in warehouse_class table are WAREHOUSE_CLASS_ID, DESCRIPTION
   * whereas projected field names are id, description.
   * In this case, we will not replace the sublist with (*)
   * @param projectedColumnsNames contains projected column names
   * @param actualColumnNames contains actual column names in a table
   * @param subListBeginEnd map which contains starting and ending index of *
   * @return boolean
   */
  private static boolean columnNamesMatchesActualFieldNames(List<String> projectedColumnsNames,
      List<String> actualColumnNames, Map<Integer, Integer> subListBeginEnd) {
    int startingIndex = 0;
    int endingIndex = 0;
    for (Map.Entry<Integer, Integer> entry : subListBeginEnd.entrySet()) {
      startingIndex = entry.getKey();
      endingIndex = entry.getValue();
    }
    return removeNumericSuffixFromColumns(projectedColumnsNames).subList(startingIndex,
        endingIndex + 1).equals(actualColumnNames);
  }

  /**
   * if projectedColumnNames contains EMPNO1, EMP_NAME2, POSITION1 as aliases.
   * we will trim the numeric suffix from the column names
   * @param projectedColumnsNames contains projected column names
   * @return List(String) containing EMPNO, EMP_NAME, POSITION
   */
  private static List<String> removeNumericSuffixFromColumns(List<String> projectedColumnsNames) {
    return projectedColumnsNames.stream()
        .map(s -> s.replaceAll("\\d*$", ""))
        .collect(Collectors.toList());
  }
  /**
   * Below method evaluates whether the projections in the given project rel can be combined or not.
   * We combine projections if
   * i) RexInputRef count in projection list is greater than or equal to the field count of input
   * ii) project does not contain window function [Row_Number over- CALCITE-3876] or case statement
   * @param projects - list of project rexNodes
   * @param input    - underlying input rel of projection
   * @return boolean
   */
  private static boolean canCombineProjects(List<RexNode> projects, RelNode input) {
    return (projects.stream().filter(p -> p instanceof RexInputRef).
        collect(Collectors.toList()).size() >= input.getRowType().getFieldCount()) && !(
        projects.stream().anyMatch(p -> RexOver.containsOver(p)
            || (p instanceof RexCall && p.getKind() == SqlKind.CASE)));
  }

  public void buildOptimizedSelectList(Project projectRel,
      Map<Integer, Integer> starProjectionSublistIndices,
      List<SqlNode> selectList, SqlImplementor.Builder builder) {
    for (int i = 0; i < projectRel.getProjects().size(); i++) {
      SqlNode sqlExpr;
      //if the current index is beginning of the flattened column-list of * , replace it with (*)
      if (starProjectionSublistIndices.containsKey(i)) {
        sqlExpr = SqlIdentifier.STAR;
        i = starProjectionSublistIndices.get(i);
      } else {
        sqlExpr = builder.context.toSql(null, projectRel.getProjects().get(i));
      }
      RelDataTypeField targetField = projectRel.getRowType().getFieldList().get(i);

      if (SqlKind.SINGLE_VALUE == sqlExpr.getKind()) {
        sqlExpr = dialect.rewriteSingleValueExpr(sqlExpr);
      }

      if (SqlUtil.isNullLiteral(sqlExpr, false)
          && targetField.getType().getSqlTypeName() != SqlTypeName.NULL) {
        sqlExpr = castNullType(sqlExpr, targetField.getType());
      }
      addToSelect(projectRel, selectList, i, sqlExpr);
    }
  }

  private void addToSelect(Project e, List<SqlNode> selectList, int ordinal, SqlNode node) {
    String fieldName = e.getRowType().getFieldNames().get(ordinal);
    String alias = SqlValidatorUtil.getAlias(node, -1);
    final String lowerName = fieldName.toLowerCase(Locale.ROOT);
    if (node == SqlIdentifier.STAR) {
      fieldName = "";
    }

    if (lowerName.startsWith("expr$")) {
      // Put it in ordinalMap
      this.sqlImplementor.ordinalMap.put(lowerName, node);
    } else if (alias == null || !alias.equals(fieldName)) {
      node = sqlImplementor.as(node, fieldName);
    }

    selectList.add(node);
  }

  protected static boolean hasOptimizedStarInProjection(int ordinal, SqlNodeList selectList,
      List<SqlNode> originalList) {
    return (ordinal > selectList.size() - 1)
        || ((originalList.size() > selectList.size())
        && selectList.stream().
        anyMatch(
            it -> it instanceof SqlIdentifier
                && it.toString().equals("*")));
  }

  protected static boolean containsStarInSelectList(SqlNodeList selectList) {
    return selectList.stream().anyMatch(item -> item.toString().equals("*"));
  }

  public SqlNode castNullType(SqlNode nullLiteral, RelDataType type) {
    final SqlNode typeNode = dialect.getCastSpec(type);
    if (typeNode == null) {
      return nullLiteral;
    }
    return SqlStdOperatorTable.CAST.createCall(POS, nullLiteral, typeNode);
  }
}

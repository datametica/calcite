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

import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.config.QueryStyle;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.CTEDefinationTrait;
import org.apache.calcite.plan.CTEDefinationTraitDef;
import org.apache.calcite.plan.PivotRelTrait;
import org.apache.calcite.plan.PivotRelTraitDef;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static org.apache.calcite.rex.RexLiteral.stringValue;

import static java.util.Objects.requireNonNull;

/**
 * Utility to convert relational expressions to SQL abstract syntax tree.
 */
public class RelToSqlConverter extends SqlImplementor
    implements ReflectiveVisitor {
  private final ReflectUtil.MethodDispatcher<Result> dispatcher;

  private final Deque<Frame> stack = new ArrayDeque<>();
  private QueryStyle style;
  /** Creates a RelToSqlConverter. */
  @SuppressWarnings("argument.type.incompatible")
  public RelToSqlConverter(SqlDialect dialect) {
    super(dialect, DEFAULT_BLOAT);
    style = new QueryStyle();
    dispatcher = ReflectUtil.createMethodDispatcher(Result.class, this, "visit",
      RelNode.class);
  }
  public RelToSqlConverter(SqlDialect dialect, QueryStyle style) {
    super(dialect, DEFAULT_BLOAT);
    this.style = style;
    dispatcher = ReflectUtil.createMethodDispatcher(Result.class, this, "visit",
        RelNode.class);
  }

  public RelToSqlConverter(SqlDialect dialect, int bloat) {
    this(dialect, new QueryStyle(), bloat);
  }

  public RelToSqlConverter(SqlDialect dialect, QueryStyle style, int bloat) {
    super(dialect, bloat);
    this.style = style;
    dispatcher = ReflectUtil.createMethodDispatcher(Result.class, this, "visit",
        RelNode.class);
  }

  /** Dispatches a call to the {@code visit(Xxx e)} method where {@code Xxx}
   * most closely matches the runtime type of the argument. */
  protected Result dispatch(RelNode e) {
    return dispatcher.invoke(e);
  }

  @Override public Result visitInput(RelNode parent, int i, boolean anon,
                                     boolean ignoreClauses, Set<Clause> expectedClauses) {
    try {
      final RelNode e = parent.getInput(i);
      stack.push(new Frame(parent, i, e, anon, ignoreClauses, expectedClauses));
      return dispatch(e);
    } finally {
      stack.pop();
    }
  }

  @Override protected boolean isAnon() {
    Frame peek = stack.peek();
    return peek == null || peek.anon;
  }

  @Override protected Result result(SqlNode node, Collection<Clause> clauses,
      @Nullable String neededAlias, @Nullable RelDataType neededType,
      Map<String, RelDataType> aliases) {
    final Frame frame = requireNonNull(stack.peek());
    return super.result(node, clauses, neededAlias, neededType, aliases)
        .withAnon(isAnon())
        .withExpectedClauses(frame.ignoreClauses, frame.expectedClauses,
            frame.parent);
  }

  /** Visits a RelNode; called by {@link #dispatch} via reflection. */
  public Result visit(RelNode e) {
    throw new AssertionError("Need to implement " + e.getClass().getName());
  }

  /** Visits a Join; called by {@link #dispatch} via reflection. */
  public Result visit(Join e) {
    switch (e.getJoinType()) {
    case ANTI:
    case SEMI:
      return visitAntiOrSemiJoin(e);
    default:
      break;
    }
    final Result leftResult = visitInput(e, 0).resetAlias();

    //parseCorrelTable(RelNode, Result) call will save your correlation variable
    //with your alias in map

    parseCorrelTable(e, leftResult);
    final Result rightResult = visitInput(e, 1).resetAlias();
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();
    SqlNode sqlCondition = null;
    SqlLiteral condType = JoinConditionType.ON.symbol(POS);
    JoinType joinType = joinType(e.getJoinType());
    JoinType currentDialectJoinType = dialect.emulateJoinTypeForCrossJoin();
    if (isCrossJoin(e) && currentDialectJoinType != JoinType.INNER) {
      joinType = currentDialectJoinType;
      condType = JoinConditionType.NONE.symbol(POS);
    } else if (isUsingOperator(e)) {
      Map<SqlNode, SqlNode> usingSourceTargetMap = new LinkedHashMap<>();
      boolean isValidUsing = checkForValidUsingOperands(e.getCondition(), leftContext,
          rightContext, usingSourceTargetMap);
      if (isValidUsing) {
        List<SqlNode> usingNodeList = new ArrayList<>();
        for (SqlNode usingNode : usingSourceTargetMap.values()) {
          String name = ((SqlIdentifier) usingNode).names.size() > 1
              ? ((SqlIdentifier) usingNode).names.get(1) : ((SqlIdentifier) usingNode).names.get(0);
          usingNodeList.add(new SqlIdentifier(name, POS));
        }
        sqlCondition = new SqlNodeList(usingNodeList, POS);
        condType = JoinConditionType.USING.symbol(POS);
      } else {
        sqlCondition = processOperandsForONCondition(usingSourceTargetMap);
      }
    } else {
      sqlCondition = convertConditionToSqlNode(e.getCondition(),
          leftContext,
          rightContext,
          e.getLeft().getRowType().getFieldCount(),
          dialect);
    }
    SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType.symbol(POS),
            rightResult.asFrom(),
            condType,
            sqlCondition);
    return result(join, leftResult, rightResult);
  }

  private boolean isUsingOperator(Join e) {
    return RexCall.class.isInstance(e.getCondition())
        && ((RexCall) e.getCondition()).getOperator() == SqlLibraryOperators.USING;
  }

  private boolean checkForValidUsingOperands(RexNode condition, Context leftContext,
      Context rightContext,  Map<SqlNode, SqlNode> usingSourceTargetMap) {
    List<RexNode> usingOperands = ((RexCall) condition).getOperands();
    boolean isValidUsing = true;
    Context joinContext =
        leftContext.implementor().joinContext(leftContext, rightContext);
    for (RexNode usingOp : usingOperands) {
      RexNode sourceRex = ((RexCall) usingOp).operands.get(0);
      RexNode targetRex = ((RexCall) usingOp).operands.get(1);

      SqlNode sourceNode = leftContext.toSql(null, sourceRex);
      SqlNode targetNode = joinContext.toSql(null, targetRex);
      usingSourceTargetMap.put(sourceNode, targetNode);

      String sourceName = ((SqlIdentifier) sourceNode).names.size() > 1
          ? ((SqlIdentifier) sourceNode).names.get(1) : ((SqlIdentifier) sourceNode).names.get(0);
      String targetName = ((SqlIdentifier) targetNode).names.size() > 1
          ? ((SqlIdentifier) targetNode).names.get(1) : ((SqlIdentifier) targetNode).names.get(0);
      isValidUsing = isValidUsing && Objects.equals(sourceName, targetName);
    }

    return isValidUsing;
  }

  private SqlNode processOperandsForONCondition(Map<SqlNode, SqlNode> usingSourceTargetMap) {
    List<SqlNode> equalOperands = new ArrayList<>();
    for (Map.Entry<SqlNode, SqlNode> entry : usingSourceTargetMap.entrySet()) {
      List<SqlNode> operands = new ArrayList<>();
      operands.add(entry.getKey());
      operands.add(entry.getValue());
      equalOperands.add(SqlStdOperatorTable.EQUALS.createCall(new SqlNodeList(operands, POS)));
    }
    return equalOperands.size() > 1
        ? SqlStdOperatorTable.AND.createCall(new SqlNodeList(equalOperands, POS))
        : equalOperands.get(0);
  }


  protected Result visitAntiOrSemiJoin(Join e) {
    final Result leftResult = visitInput(e, 0).resetAlias();
    final Result rightResult = visitInput(e, 1).resetAlias();
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();

    SqlSelect sqlSelect;
    SqlNode sqlCondition = convertConditionToSqlNode(e.getCondition(),
        leftContext,
        rightContext,
        e.getLeft().getRowType().getFieldCount(),
        dialect);
    if (leftResult.neededAlias != null) {
      sqlSelect = leftResult.subSelect();
    } else {
      sqlSelect = leftResult.asSelect();
    }
    SqlNode fromPart = rightResult.asFrom();
    SqlSelect existsSqlSelect;
    if (fromPart.getKind() == SqlKind.SELECT) {
      existsSqlSelect = (SqlSelect) fromPart;
      existsSqlSelect.setSelectList(
          new SqlNodeList(ImmutableList.of(SqlLiteral.createExactNumeric("1", POS)), POS));
      if (existsSqlSelect.getWhere() != null) {
        sqlCondition = SqlStdOperatorTable.AND.createCall(POS,
            existsSqlSelect.getWhere(),
            sqlCondition);
      }
      existsSqlSelect.setWhere(sqlCondition);
    } else {
      existsSqlSelect =
          new SqlSelect(POS, null,
              new SqlNodeList(
                  ImmutableList.of(SqlLiteral.createExactNumeric("1", POS)), POS),
              fromPart, sqlCondition, null,
              null, null, null, null, null, null);
    }
    sqlCondition = SqlStdOperatorTable.EXISTS.createCall(POS, existsSqlSelect);
    if (e.getJoinType() == JoinRelType.ANTI) {
      sqlCondition = SqlStdOperatorTable.NOT.createCall(POS, sqlCondition);
    }
    if (sqlSelect.getWhere() != null) {
      sqlCondition = SqlStdOperatorTable.AND.createCall(POS,
          sqlSelect.getWhere(),
          sqlCondition);
    }
    sqlSelect.setWhere(sqlCondition);
    return result(sqlSelect, leftResult, rightResult);
  }

  private static boolean isCrossJoin(final Join e) {
    return e.getJoinType() == JoinRelType.INNER && e.getCondition().isAlwaysTrue();
  }

  /** Visits a Correlate; called by {@link #dispatch} via reflection. */
  public Result visit(Correlate e) {
    Result leftResult = visitInput(e, 0);
    parseCorrelTable(e, leftResult);
    final Result rightResult = visitInput(e, 1);
    SqlNode rightLateralAs = rightResult.asFrom();
    SqlNode rightNode = rightResult.node;
    if (rightNode.getKind() == SqlKind.AS) {
      rightNode = ((SqlBasicCall) rightNode).getOperands()[0];
    }

    //Following validation checks if the right evaluated node is UNNEST or not, because
    //as per ANSI standard, we either can use LATERAL with subquery or UNNEST with array/multiset
    //But both are not allowed at the same time.

    if (!(rightNode.getKind() == SqlKind.UNNEST)) {
      final SqlNode rightLateral =
          SqlStdOperatorTable.LATERAL.createCall(POS, rightResult.node);
      rightLateralAs =
          SqlStdOperatorTable.AS.createCall(POS, rightLateral,
              new SqlIdentifier(
                  requireNonNull(rightResult.neededAlias,
                      () -> "rightResult.neededAlias is null, node is " + rightResult.node), POS));
    }

    final SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            JoinType.COMMA.symbol(POS),
            rightLateralAs,
            JoinConditionType.NONE.symbol(POS),
            null);
    return result(join, leftResult, rightResult);
  }

  /** Visits a Filter; called by {@link #dispatch} via reflection. */
  public Result visit(Filter e) {
    RelToSqlUtils relToSqlUtils = new RelToSqlUtils();
    final RelNode input = e.getInput();
    if (dialect.supportsQualifyClause()
        && relToSqlUtils.hasAnalyticalFunctionInFilter(e, input)
        && !(relToSqlUtils.hasAnalyticalFunctionInJoin(input))) {
      // need to keep where clause as is if input rel of the filter rel is a LogicalJoin
      // ignoreClauses will always be true because in case of false, new select wrap gets applied
      // with this current Qualify filter e. So, the input query won't remain as it is.
      final Result x = visitInput(e, 0, isAnon(), true,
          ImmutableSet.of(Clause.QUALIFY));
      parseCorrelTable(e, x);
      final Builder builder = x.builder(e);
      builder.setQualify(builder.context.toSql(null, e.getCondition()));
      return builder.result();
    } else if (input instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) input;
      final boolean ignoreClauses = aggregate.getInput() instanceof Project;
      final Result x = visitInput(e, 0, isAnon(), ignoreClauses,
          ImmutableSet.of(Clause.HAVING));
      parseCorrelTable(e, x);
      final Builder builder = x.builder(e);
      builder.setHaving(builder.context.toSql(null, e.getCondition()));
      return builder.result();
    } else {
      final Result x = visitInput(e, 0, Clause.WHERE);
      parseCorrelTable(e, x);
      final Builder builder = x.builder(e);
      SqlNode filterNode = builder.context.toSql(null, e.getCondition());
      UnpivotRelToSqlUtil unpivotRelToSqlUtil = new UnpivotRelToSqlUtil();
      if (dialect.supportsUnpivot()
          && unpivotRelToSqlUtil.isRelEquivalentToUnpivotExpansionWithExcludeNulls
          (filterNode, x.node)) {
        SqlNode sqlUnpivot = createUnpivotSqlNodeWithExcludeNulls((SqlSelect) x.node);
        SqlNode select = new SqlSelect(
            SqlParserPos.ZERO, null, null, sqlUnpivot,
            null, null, null, null, null, null, null, SqlNodeList.EMPTY);
        return result(select, ImmutableList.of(Clause.SELECT), e, null);
      } else {
        builder.setWhere(filterNode);
        return builder.result();
      }
    }
  }

  SqlNode createUnpivotSqlNodeWithExcludeNulls(SqlSelect sqlNode) {
    SqlUnpivot sqlUnpivot = (SqlUnpivot) sqlNode.getFrom();
    assert sqlUnpivot != null;
    return new SqlUnpivot(POS, sqlUnpivot.query, false, sqlUnpivot.measureList,
        sqlUnpivot.axisList, sqlUnpivot.inList);
  }

  /** Visits a Project; called by {@link #dispatch} via reflection. */
  public Result visit(Project e) {
    UnpivotRelToSqlUtil unpivotRelToSqlUtil = new UnpivotRelToSqlUtil();
    final Result x = visitInput(e, 0, Clause.SELECT);
    final Builder builder = x.builder(e);
    Result result = null;
    if (dialect.supportsUnpivot()
        && unpivotRelToSqlUtil.isRelEquivalentToUnpivotExpansionWithIncludeNulls(e, builder)) {
      SqlUnpivot sqlUnpivot = createUnpivotSqlNodeWithIncludeNulls(e, builder, unpivotRelToSqlUtil);
      SqlNode select = new SqlSelect(
          SqlParserPos.ZERO, null, builder.select.getSelectList(), sqlUnpivot,
          null, null, null, null, null, null, null, SqlNodeList.EMPTY);
      result = result(select, ImmutableList.of(Clause.SELECT), e, null);
    } else {
      parseCorrelTable(e, x);
      if ((!isStar(e.getProjects(), e.getInput().getRowType(), e.getRowType())
          || style.isExpandProjection()) && !unpivotRelToSqlUtil.isStarInUnPivot(e, x)) {
        final List<SqlNode> selectList = new ArrayList<>();
        if (!StarProjectionUtils.identifyStarProjectionSublistIndices(
            e.getProjects(), e.getInput(), e).isEmpty()) {
          buildOptimizedSelectList(e, selectList, builder);
          originalList.clear();
          populateOriginalList(e, builder);
        } else {
          for (RexNode ref : e.getProjects()) {
            SqlNode sqlExpr = builder.context.toSql(null, ref);
            RelDataTypeField targetField = e.getRowType().getFieldList().get(selectList.size());

            if (SqlKind.SINGLE_VALUE == sqlExpr.getKind()) {
              sqlExpr = dialect.rewriteSingleValueExpr(sqlExpr);
            }

            if (SqlUtil.isNullLiteral(sqlExpr, false)
                && targetField.getType().getSqlTypeName() != SqlTypeName.NULL) {
              sqlExpr = castNullType(sqlExpr, targetField.getType());
            }
            addSelect(selectList, sqlExpr, e.getRowType());
          }
        }

        builder.setSelect(new SqlNodeList(selectList, POS));
      }
      result = builder.result();
    }
    if (CTERelToSqlUtil.isCteScopeTrait(e.getTraitSet())
        || CTERelToSqlUtil.isCteDefinationTrait(e.getTraitSet())) {
      return updateCTEResult(e, result);
    }
    return result;
  }

  private void buildOptimizedSelectList(Project e, List<SqlNode> selectList, Builder builder) {
    Map<Integer, Integer> starProjectionSublistIndices = StarProjectionUtils.
        identifyStarProjectionSublistIndices(e.getProjects(), e.getInput(), e);
    StarProjectionUtils starProjectionUtils = new StarProjectionUtils(this);
    starProjectionUtils.buildOptimizedSelectList(e,
        starProjectionSublistIndices, selectList, builder);
  }

  private void populateOriginalList(Project e, Builder builder) {
    for (RexNode ref : e.getProjects()) {
      SqlNode sqlExpr = builder.context.toSql(null, ref);
      if (originalList.size() < e.getRowType().getFieldCount()) {
        addSelect(originalList, sqlExpr, e.getRowType());
      }
    }
  }

  /**
   * Create {@link SqlUnpivot} type of SqlNode.
   */
  private SqlUnpivot createUnpivotSqlNodeWithIncludeNulls(Project projectRel,
      SqlImplementor.Builder builder, UnpivotRelToSqlUtil unpivotRelToSqlUtil) {
    RelNode leftRelOfJoin = ((LogicalJoin) projectRel.getInput(0)).getLeft();
    SqlNode query = dispatch(leftRelOfJoin).asStatement();
    LogicalValues valuesRel = unpivotRelToSqlUtil.getLogicalValuesRel(projectRel);
    SqlNodeList axisList = new SqlNodeList(ImmutableList.of
        (new SqlIdentifier(unpivotRelToSqlUtil.getLogicalValueAlias(valuesRel), POS)), POS);
    List<SqlIdentifier> measureColumnSqlIdentifiers = new ArrayList<>();
    Map<String, SqlNodeList> caseAliasVsThenList = unpivotRelToSqlUtil.
        getCaseAliasVsThenList(projectRel, builder);
    for (String axisColumn : new ArrayList<>(caseAliasVsThenList.keySet())) {
      measureColumnSqlIdentifiers.add(new SqlIdentifier(axisColumn, POS));
    }
    SqlNodeList measureList = new SqlNodeList(measureColumnSqlIdentifiers, POS);
    SqlNodeList aliasOfInList = unpivotRelToSqlUtil.getLogicalValuesList(valuesRel, builder);
    SqlNodeList inSqlNodeList = new SqlNodeList(caseAliasVsThenList.values(),
        POS);
    SqlNodeList aliasedInSqlNodeList = unpivotRelToSqlUtil.
        getInListForSqlUnpivot(measureList, aliasOfInList,
        inSqlNodeList);
    return new SqlUnpivot(POS, query, true, measureList, axisList, aliasedInSqlNodeList);
  }

    /** Wraps a NULL literal in a CAST operator to a target type.
     *
     * @param nullLiteral NULL literal
     * @param type Target type
     *
     * @return null literal wrapped in CAST call
     */
  public SqlNode castNullType(SqlNode nullLiteral, RelDataType type) {
    final SqlNode typeNode = dialect.getCastSpec(type);
    if (typeNode == null) {
      return nullLiteral;
    }
    return SqlStdOperatorTable.CAST.createCall(POS, nullLiteral, typeNode);
  }

  /** Visits a Window; called by {@link #dispatch} via reflection. */
  public Result visit(Window e) {
    final Result x = visitInput(e, 0);
    final Builder builder = x.builder(e);
    final RelNode input = e.getInput();
    final int inputFieldCount = input.getRowType().getFieldCount();
    final List<SqlNode> rexOvers = new ArrayList<>();
    for (Window.Group group: e.groups) {
      rexOvers.addAll(builder.context.toSql(group, e.constants, inputFieldCount));
    }
    final List<SqlNode> selectList = new ArrayList<>();

    for (RelDataTypeField field: input.getRowType().getFieldList()) {
      addSelect(selectList, builder.context.field(field.getIndex()), e.getRowType());
    }

    for (SqlNode rexOver: rexOvers) {
      addSelect(selectList, rexOver, e.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    return builder.result();
  }

  /** Visits an Aggregate; called by {@link #dispatch} via reflection. */
  public Result visit(Aggregate e) {
    final Builder builder =
        visitAggregate(e, e.getGroupSet().toList(), Clause.GROUP_BY);
    RelTrait relTrait = e.getTraitSet().getTrait(PivotRelTraitDef.instance);
    if (relTrait != null && relTrait instanceof PivotRelTrait) {
      if (((PivotRelTrait) relTrait).isPivotRel()) {
        PivotRelToSqlUtil pivotRelToSqlUtil = new PivotRelToSqlUtil(POS);
        SqlNode select =
            pivotRelToSqlUtil.buildSqlPivotNode(e, builder, builder.select.getSelectList());
        return result(select, ImmutableList.of(Clause.SELECT), e, null);
      }
    }
    Result result = builder.result();
    if (CTERelToSqlUtil.isCteScopeTrait(e.getTraitSet())
        || CTERelToSqlUtil.isCteDefinationTrait(e.getTraitSet())) {
      return updateCTEResult(e, result);
    }
    return result;
  }

  private Builder visitAggregate(Aggregate e, List<Integer> groupKeyList,
      Clause... clauses) {
    // "select a, b, sum(x) from ( ... ) group by a, b"
    boolean ignoreClauses = false;
    if (e.getInput() instanceof Project) {
      if (!(((Project) e.getInput()).getInput() instanceof Filter
          && ((Filter) ((Project) e.getInput()).getInput()).getInput() instanceof Filter)) {
        ignoreClauses = true;
      }
    }
    final Result x = visitInput(e, 0, isAnon(), ignoreClauses,
        ImmutableSet.copyOf(clauses));
    final Builder builder = x.builder(e);
    final List<SqlNode> selectList = new ArrayList<>();
    final List<SqlNode> groupByList =
        generateGroupList(builder, selectList, e, groupKeyList);
    return buildAggregate(e, builder, selectList, groupByList);
  }

  /**
   * Builds the group list for an Aggregate node.
   *
   * @param e The Aggregate node
   * @param builder The SQL builder
   * @param groupByList output group list
   * @param selectList output select list
   */
  protected void buildAggGroupList(Aggregate e, Builder builder,
      List<SqlNode> groupByList, List<SqlNode> selectList) {
    for (int group : e.getGroupSet()) {
      final SqlNode field = builder.context.field(group);
      addSelect(selectList, field, e.getRowType());
      groupByList.add(field);
    }
  }

  /**
   * Builds an aggregate query.
   *
   * @param e The Aggregate node
   * @param builder The SQL builder
   * @param selectList The precomputed group list
   * @param groupByList The precomputed select list
   * @return The aggregate query result
   */
  protected Builder buildAggregate(Aggregate e, Builder builder,
      List<SqlNode> selectList, List<SqlNode> groupByList) {
    for (AggregateCall aggCall : e.getAggCallList()) {
      SqlNode aggCallSqlNode = builder.context.toSql(aggCall);
      if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
        aggCallSqlNode = dialect.rewriteSingleValueExpr(aggCallSqlNode);
      }
      addSelect(selectList, aggCallSqlNode, e.getRowType());
    }
    if (!isStarInAggregateRel(e)) {
      builder.setSelect(new SqlNodeList(selectList, POS));
    }
    if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
      // Some databases don't support "GROUP BY ()". We can omit it as long
      // as there is at least one aggregate function.
      builder.setGroupBy(new SqlNodeList(groupByList, POS));
    }
    return builder;
  }

  /**
   * Evaluates if projection fields can be replaced with aestrisk.
   * @param e aggregate rel
   * @return true if selectList is required to be added in sqlNode
   */
  boolean isStarInAggregateRel(Aggregate e) {
    if (e.getAggCallList().size() > 0) {
      return false;
    }
    RelNode input = e.getInput();
    while (input != null) {
      if (input instanceof Project || input instanceof TableScan || input instanceof Join) {
        break;
      }
      input = input.getInput(0);
    }
    return e.getRowType().getFieldNames().equals(input.getRowType().getFieldNames());
  }

  /** Generates the GROUP BY items, for example {@code GROUP BY x, y},
   * {@code GROUP BY CUBE (x, y)} or {@code GROUP BY ROLLUP (x, y)}.
   *
   * <p>Also populates the SELECT clause. If the GROUP BY list is simple, the
   * SELECT will be identical; if the GROUP BY list contains GROUPING SETS,
   * CUBE or ROLLUP, the SELECT clause will contain the distinct leaf
   * expressions. */
  private List<SqlNode> generateGroupList(Builder builder,
      List<SqlNode> selectList, Aggregate aggregate, List<Integer> groupList) {
    final List<Integer> sortedGroupList =
        Ordering.natural().sortedCopy(groupList);
    assert aggregate.getGroupSet().asList().equals(sortedGroupList)
        : "groupList " + groupList + " must be equal to groupSet "
        + aggregate.getGroupSet() + ", just possibly a different order";

    final List<SqlNode> groupKeys = new ArrayList<>();
    for (int key : groupList) {
      groupKeys.add(getGroupBySqlNode(builder, key));
    }

    for (int key : sortedGroupList) {
      final SqlNode field = builder.context.field(key);
      // final SqlNode field = getGroupBySqlNode(builder,key);
      addSelect(selectList, field, aggregate.getRowType());
    }
    switch (aggregate.getGroupType()) {
    case SIMPLE:
      return ImmutableList.copyOf(groupKeys);
    case CUBE:
      if (aggregate.getGroupSet().cardinality() > 1) {
        return ImmutableList.of(
            SqlStdOperatorTable.CUBE.createCall(SqlParserPos.ZERO, groupKeys));
      }
      // a singleton CUBE and ROLLUP are the same but we prefer ROLLUP;
      // fall through
    case ROLLUP:
      return ImmutableList.of(
          SqlStdOperatorTable.ROLLUP.createCall(SqlParserPos.ZERO, groupKeys));
    default:
    case OTHER:
      return ImmutableList.of(
          SqlStdOperatorTable.GROUPING_SETS.createCall(SqlParserPos.ZERO,
              aggregate.getGroupSets().stream()
                  .map(groupSet ->
                      groupItem(groupKeys, groupSet, aggregate.getGroupSet()))
                  .collect(Collectors.toList())));
    }
  }


  private SqlNode getGroupBySqlNode(Builder builder, int key) {
    boolean isGroupByAlias = dialect.getConformance().isGroupByAlias();
    if (isAliasNotRequiredInGroupBy(builder, key)) {
      isGroupByAlias = false;
    }

    if (builder.select.getSelectList() == null || !isGroupByAlias) {
      return builder.context.field(key);
    } else {
      return builder.context.field(key, true);
    }
    /*SqlNode sqlNode = builder.select.getSelectList().get(key);
    if (sqlNode.getKind() == SqlKind.LITERAL
        || sqlNode.getKind() == SqlKind.DYNAMIC_PARAM
        || sqlNode.getKind() == SqlKind.MINUS_PREFIX) {
      Optional<SqlNode> aliasNode = getAliasSqlNode(sqlNode);
      if (aliasNode.isPresent()) {
        return aliasNode.get();
      } else {
        //add ordinal
        int ordinal =
            builder.select.getSelectList().getList().indexOf(sqlNode) + 1;
        return SqlLiteral.createExactNumeric(String.valueOf(ordinal),
            SqlParserPos.ZERO);
      }
    } */
  }

  private boolean isAliasNotRequiredInGroupBy(Builder builder, int key) {
    if (builder.context.field(key).getKind() == SqlKind.LITERAL
        && dialect.getConformance().isGroupByOrdinal()) {
      if (builder.select.getSelectList() != null) {
        SqlNodeList selectList = builder.select.getSelectList();
        final SqlNode selectItem = StarProjectionUtils.hasOptimizedStarInProjection(key, selectList,
            originalList) ? originalList.get(key) : selectList.get(key);
        Optional<SqlNode> aliasNode = getAliasSqlNode(selectItem);
        return !aliasNode.isPresent();
      }
      return true;
    }
    return false;
  }

  private Optional<SqlNode> getAliasSqlNode(SqlNode sqlNode) {
    if (SqlCall.class.isInstance(sqlNode)) {
      List<SqlNode> openrandList = ((SqlCall) sqlNode).getOperandList();
      if (openrandList.size() > 1 && !openrandList.get(1)
          .toString()
          .toLowerCase(Locale.ROOT)
          .startsWith("expr$")) {
        return Optional.of(openrandList.get(1));
      }
    }
    return Optional.empty();
  }

  private static SqlNode groupItem(List<SqlNode> groupKeys,
      ImmutableBitSet groupSet, ImmutableBitSet wholeGroupSet) {
    final List<SqlNode> nodes = groupSet.asList().stream()
        .map(key -> groupKeys.get(wholeGroupSet.indexOf(key)))
        .collect(Collectors.toList());
    switch (nodes.size()) {
    case 1:
      return nodes.get(0);
    default:
      return SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, nodes);
    }
  }

  /** Visits a TableScan; called by {@link #dispatch} via reflection. */
  public Result visit(TableScan e) {
    final SqlIdentifier identifier = getSqlTargetTable(e);
    return result(identifier, ImmutableList.of(Clause.FROM), e, null);
  }

  /** Visits a Union; called by {@link #dispatch} via reflection. */
  public Result visit(Union e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.UNION_ALL
        : SqlStdOperatorTable.UNION, e);
  }

  /** Visits an Intersect; called by {@link #dispatch} via reflection. */
  public Result visit(Intersect e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.INTERSECT_ALL
        : SqlStdOperatorTable.INTERSECT, e);
  }

  /** Visits a Minus; called by {@link #dispatch} via reflection. */
  public Result visit(Minus e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.EXCEPT_ALL
        : SqlStdOperatorTable.EXCEPT, e);
  }

  /** Visits a Calc; called by {@link #dispatch} via reflection. */
  public Result visit(Calc e) {
    final RexProgram program = e.getProgram();
    final ImmutableSet<Clause> expectedClauses =
        program.getCondition() != null
            ? ImmutableSet.of(Clause.WHERE)
            : ImmutableSet.of();
    final Result x = visitInput(e, 0, expectedClauses);
    parseCorrelTable(e, x);
    final Builder builder = x.builder(e);
    if (!isStar(program)) {
      final List<SqlNode> selectList = new ArrayList<>(program.getProjectList().size());
      for (RexLocalRef ref : program.getProjectList()) {
        SqlNode sqlExpr = builder.context.toSql(program, ref);
        addSelect(selectList, sqlExpr, e.getRowType());
      }
      builder.setSelect(new SqlNodeList(selectList, POS));
    }

    if (program.getCondition() != null) {
      builder.setWhere(
          builder.context.toSql(program, program.getCondition()));
    }
    return builder.result();
  }

  /** Visits a Values; called by {@link #dispatch} via reflection. */
  public Result visit(Values e) {
    final List<Clause> clauses = ImmutableList.of(Clause.SELECT);
    final Map<String, RelDataType> pairs = ImmutableMap.of();
    final Context context = aliasContext(pairs, false);
    SqlNode query;
    final boolean rename = stack.size() <= 1
        || !(Iterables.get(stack, 1).r instanceof TableModify);
    final List<String> fieldNames = e.getRowType().getFieldNames();
    if (!dialect.supportsAliasedValues() && rename) {
      // Some dialects (such as Oracle and BigQuery) don't support
      // "AS t (c1, c2)". So instead of
      //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
      // we generate
      //   SELECT v0 AS c0, v1 AS c1 FROM DUAL
      //   UNION ALL
      //   SELECT v2 AS c0, v3 AS c1 FROM DUAL
      // for Oracle and
      //   SELECT v0 AS c0, v1 AS c1
      //   UNION ALL
      //   SELECT v2 AS c0, v3 AS c1
      // for dialects that support SELECT-without-FROM.
      List<SqlSelect> list = new ArrayList<>();
      for (List<RexLiteral> tuple : e.getTuples()) {
        final List<SqlNode> values2 = new ArrayList<>();
        final SqlNodeList exprList = exprList(context, tuple);
        for (Pair<SqlNode, String> value : Pair.zip(exprList, fieldNames)) {
          values2.add(as(value.left, value.right));
        }
        list.add(
            new SqlSelect(POS, null,
                new SqlNodeList(values2, POS),
                getDual(), null, null,
                null, null, null, null, null, null));
      }
      if (list.isEmpty()) {
        // In this case we need to construct the following query:
        // SELECT NULL as C0, NULL as C1, NULL as C2 ... FROM DUAL WHERE FALSE
        // This would return an empty result set with the same number of columns as the field names.
        final List<SqlNode> nullColumnNames = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
          SqlCall nullColumnName = as(SqlLiteral.createNull(POS), fieldName);
          nullColumnNames.add(nullColumnName);
        }
        final SqlIdentifier dual = getDual();
        if (dual == null) {
          query = new SqlSelect(POS, null,
              new SqlNodeList(nullColumnNames, POS), null, null, null, null,
              null, null, null, null, null);

          // Wrap "SELECT 1 AS x"
          // as "SELECT * FROM (SELECT 1 AS x) AS t WHERE false"
          query = new SqlSelect(POS, null, SqlNodeList.SINGLETON_STAR,
              as(query, "t"), createAlwaysFalseCondition(), null, null,
              null, null, null, null, null);
        } else {
          query = new SqlSelect(POS, null,
              new SqlNodeList(nullColumnNames, POS),
              dual, createAlwaysFalseCondition(), null,
              null, null, null, null, null, null);
        }
      } else if (list.size() == 1) {
        query = list.get(0);
      } else {
        SqlNode sqlNode = SqlStdOperatorTable.UNION_ALL.createCall(POS, list.get(0), list.get(1));
        for (int i = 2; i < list.size(); i++) {
          sqlNode = SqlStdOperatorTable.UNION_ALL.createCall(POS, sqlNode, list.get(i));
        }
        query = sqlNode;
      }
    } else {
      // Generate ANSI syntax
      //   (VALUES (v0, v1), (v2, v3))
      // or, if rename is required
      //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
      final SqlNodeList selects = new SqlNodeList(POS);
      final boolean isEmpty = Values.isEmpty(e);
      if (isEmpty) {
        // In case of empty values, we need to build:
        //   SELECT *
        //   FROM (VALUES (NULL, NULL ...)) AS T (C1, C2 ...)
        //   WHERE 1 = 0
        selects.add(
            SqlInternalOperators.ANONYMOUS_ROW.createCall(POS,
                Collections.nCopies(fieldNames.size(),
                    SqlLiteral.createNull(POS))));
      } else {
        for (List<RexLiteral> tuple : e.getTuples()) {
          selects.add(
              SqlInternalOperators.ANONYMOUS_ROW.createCall(
                  exprList(context, tuple)));
        }
      }
      query = SqlStdOperatorTable.VALUES.createCall(selects);
      if (rename) {
        query = as(query, "t", fieldNames.toArray(new String[0]));
      }
      if (isEmpty) {
        if (!rename) {
          query = as(query, "t");
        }
        query = new SqlSelect(POS, null,
                null, query,
                createAlwaysFalseCondition(),
                null, null, null,
                null, null, null, null);
      }
    }
    return result(query, clauses, e, null);
  }

  private @Nullable SqlIdentifier getDual() {
    final List<String> names = dialect.getSingleRowTableName();
    if (names == null) {
      return null;
    }
    return new SqlIdentifier(names, POS);
  }

  private static SqlNode createAlwaysFalseCondition() {
    // Building the select query in the form:
    // select * from VALUES(NULL,NULL ...) where 1=0
    // Use condition 1=0 since "where false" does not seem to be supported
    // on some DB vendors.
    return SqlStdOperatorTable.EQUALS.createCall(POS,
            ImmutableList.of(SqlLiteral.createExactNumeric("1", POS),
                    SqlLiteral.createExactNumeric("0", POS)));
  }

  /** Visits a Sort; called by {@link #dispatch} via reflection. */
  public Result visit(Sort e) {
    Result result = null;
    if (e.getInput() instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) e.getInput();
      if (hasTrickyRollup(e, aggregate)) {
        // MySQL 5 does not support standard "GROUP BY ROLLUP(x, y)", only
        // the non-standard "GROUP BY x, y WITH ROLLUP".
        // It does not allow "WITH ROLLUP" in combination with "ORDER BY",
        // but "GROUP BY x, y WITH ROLLUP" implicitly sorts by x, y,
        // so skip the ORDER BY.
        final Set<Integer> groupList = new LinkedHashSet<>();
        for (RelFieldCollation fc : e.collation.getFieldCollations()) {
          groupList.add(aggregate.getGroupSet().nth(fc.getFieldIndex()));
        }
        groupList.addAll(Aggregate.Group.getRollup(aggregate.getGroupSets()));
        final Builder builder =
            visitAggregate(aggregate, ImmutableList.copyOf(groupList),
                Clause.GROUP_BY, Clause.OFFSET, Clause.FETCH);
        offsetFetch(e, builder);
        result = builder.result();
        if (CTERelToSqlUtil.isCteScopeTrait(e.getTraitSet())
            || CTERelToSqlUtil.isCteDefinationTrait(e.getTraitSet())) {
          return updateCTEResult(e, result);
        }
        return result;
      }
    }
    if (e.getInput() instanceof Project) {
      // Deal with the case Sort(Project(Aggregate ...))
      // by converting it to Project(Sort(Aggregate ...)).
      final Project project = (Project) e.getInput();
      final Permutation permutation = project.getPermutation();
      if (permutation != null
          && project.getInput() instanceof Aggregate) {
        final Aggregate aggregate = (Aggregate) project.getInput();
        if (hasTrickyRollup(e, aggregate)) {
          final RelCollation collation =
              RelCollations.permute(e.collation, permutation);
          final Sort sort2 =
              LogicalSort.create(aggregate, collation, e.offset, e.fetch);
          final Project project2 =
              LogicalProject.create(
                  sort2,
                  ImmutableList.of(),
                  project.getProjects(),
                  project.getRowType(),
                  project.getVariablesSet());
          return visit(project2);
        }
      }
    }
    final Result x = visitInput(e, 0, Clause.ORDER_BY, Clause.OFFSET,
        Clause.FETCH);
    final Builder builder = x.builder(e);
    if (stack.size() != 1 && builder.select.getSelectList() == null) {
      // Generates explicit column names instead of start(*) for
      // non-root order by to avoid ambiguity.
      final List<SqlNode> selectList = Expressions.list();
      for (RelDataTypeField field : e.getRowType().getFieldList()) {
        addSelect(selectList, builder.context.field(field.getIndex()), e.getRowType());
      }
      builder.select.setSelectList(new SqlNodeList(selectList, POS));
    }
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      builder.addOrderItem(orderByList, field);
    }
    if (!orderByList.isEmpty()) {
      builder.setOrderBy(new SqlNodeList(orderByList, POS));
    }
    offsetFetch(e, builder);
    result = builder.result();
    if (CTERelToSqlUtil.isCteScopeTrait(e.getTraitSet())
        || CTERelToSqlUtil.isCteDefinationTrait(e.getTraitSet())) {
      return updateCTEResult(e, result);
    }
    return result;
  }

  Result updateCTEResult(RelNode e, Result result) {

    // CTE Scope Trait
    if (CTERelToSqlUtil.isCteScopeTrait(e.getTraitSet())) {
      List<SqlNode> sqlWithItemNodes = CTERelToSqlUtil.fetchSqlWithItemNodes(result.node,
          new ArrayList<>());
      SqlNodeList sqlNodeList = new SqlNodeList(sqlWithItemNodes, POS);

      SqlNode sqlWithNode = updateSqlWithNode(result);
      final SqlWith sqlWith = new SqlWith(POS, sqlNodeList, sqlWithNode);
      result = this.result(sqlWith, ImmutableList.of(), e, null);
    } else if (CTERelToSqlUtil.isCteDefinationTrait(e.getTraitSet())) {
      //CTE Definition Trait
      RelTrait relDefinationTrait = e.getTraitSet().getTrait(CTEDefinationTraitDef.instance);
      CTEDefinationTrait cteDefinationTrait = (CTEDefinationTrait) relDefinationTrait;
      SqlIdentifier withName = new SqlIdentifier(cteDefinationTrait.getCteName(), POS);

      SqlNodeList columnList = identifierList(new ArrayList<>());
      SqlWithItem sqlWithItem = new SqlWithItem(POS, withName, columnList, result.node);

      Map<String, RelDataType> aliasesMap = new HashMap<>();
      List<RelDataTypeField> fieldList = e.getRowType().getFieldList();
      RelDataTypeField relDataTypeField = fieldList.get(0);
      aliasesMap.put(relDataTypeField.getName(), e.getRowType());

      result = this.result(sqlWithItem, result.clauses, e, aliasesMap);
    }
    return result;
  }

  /** Adds OFFSET and FETCH to a builder, if applicable.
   * The builder must have been created with OFFSET and FETCH clauses. */
  void offsetFetch(Sort e, Builder builder) {
    if (e.fetch != null) {
      builder.setFetch(builder.context.toSql(null, e.fetch));
    }
    if (e.offset != null) {
      builder.setOffset(builder.context.toSql(null, e.offset));
    }
  }

  public boolean hasTrickyRollup(Sort e, Aggregate aggregate) {
    return !dialect.supportsAggregateFunction(SqlKind.ROLLUP)
        && dialect.supportsGroupByWithRollup()
        && (aggregate.getGroupType() == Aggregate.Group.ROLLUP
            || aggregate.getGroupType() == Aggregate.Group.CUBE
                && aggregate.getGroupSet().cardinality() == 1)
        && e.collation.getFieldCollations().stream().allMatch(fc ->
            fc.getFieldIndex() < aggregate.getGroupSet().cardinality());
  }

  private static SqlIdentifier getSqlTargetTable(RelNode e) {
    // Use the foreign catalog, schema and table names, if they exist,
    // rather than the qualified name of the shadow table in Calcite.
    final RelOptTable table = requireNonNull(e.getTable());
    return table.maybeUnwrap(JdbcTable.class)
        .map(JdbcTable::tableName)
        .orElseGet(() ->
            new SqlIdentifier(table.getQualifiedName(), SqlParserPos.ZERO));
  }

  /** Visits a TableModify; called by {@link #dispatch} via reflection. */
  public Result visit(TableModify modify) {
    final Map<String, RelDataType> pairs = ImmutableMap.of();
    final Context context = aliasContext(pairs, false);

    // Target Table Name
    final SqlIdentifier sqlTargetTable = getSqlTargetTable(modify);

    switch (modify.getOperation()) {
    case INSERT: {
      // Convert the input to a SELECT query or keep as VALUES. Not all
      // dialects support naked VALUES, but all support VALUES inside INSERT.
      final SqlNode sqlSource =
          visitInput(modify, 0).asQueryOrValues();

      final SqlInsert sqlInsert =
          new SqlInsert(POS, SqlNodeList.EMPTY, sqlTargetTable, sqlSource,
              identifierList(modify.getTable().getRowType().getFieldNames()));

      return result(sqlInsert, ImmutableList.of(), modify, null);
    }
    case UPDATE: {
      final Result input = visitInput(modify, 0);

      final SqlUpdate sqlUpdate =
          new SqlUpdate(POS, sqlTargetTable,
              identifierList(
                  requireNonNull(modify.getUpdateColumnList(),
                      () -> "modify.getUpdateColumnList() is null for " + modify)),
              exprList(context,
                  requireNonNull(modify.getSourceExpressionList(),
                      () -> "modify.getSourceExpressionList() is null for " + modify)),
              ((SqlSelect) input.node).getWhere(), input.asSelect(),
              null);

      return result(sqlUpdate, input.clauses, modify, null);
    }
    case DELETE: {
      final Result input = visitInput(modify, 0);

      final SqlDelete sqlDelete =
          new SqlDelete(POS, sqlTargetTable,
              input.asSelect().getWhere(), input.asSelect(), null);

      return result(sqlDelete, input.clauses, modify, null);
    }
    case MERGE:
    default:
      throw new AssertionError("not implemented: " + modify);
    }
  }

  /** Converts a list of {@link RexNode} expressions to {@link SqlNode}
   * expressions. */
  private static SqlNodeList exprList(final Context context,
      List<? extends RexNode> exprs) {
    return new SqlNodeList(
        Util.transform(exprs, e -> context.toSql(null, e)), POS);
  }

  /** Converts a list of names expressions to a list of single-part
   * {@link SqlIdentifier}s. */
  private static SqlNodeList identifierList(List<String> names) {
    return new SqlNodeList(
        Util.transform(names, name -> new SqlIdentifier(name, POS)), POS);
  }

  /** Visits a Match; called by {@link #dispatch} via reflection. */
  public Result visit(Match e) {
    final RelNode input = e.getInput();
    final Result x = visitInput(e, 0);
    final Context context = matchRecognizeContext(x.qualifiedContext());

    SqlNode tableRef = x.asQueryOrValues();

    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    final List<SqlNode> partitionSqlList = new ArrayList<>();
    for (int key : e.getPartitionKeys()) {
      final RexInputRef ref = rexBuilder.makeInputRef(input, key);
      SqlNode sqlNode = context.toSql(null, ref);
      partitionSqlList.add(sqlNode);
    }
    final SqlNodeList partitionList = new SqlNodeList(partitionSqlList, POS);

    final List<SqlNode> orderBySqlList = new ArrayList<>();
    if (e.getOrderKeys() != null) {
      for (RelFieldCollation fc : e.getOrderKeys().getFieldCollations()) {
        if (fc.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED) {
          boolean first = fc.nullDirection == RelFieldCollation.NullDirection.FIRST;
          SqlNode nullDirectionNode =
              dialect.emulateNullDirection(context.field(fc.getFieldIndex()),
                  first, fc.direction.isDescending());
          if (nullDirectionNode != null) {
            orderBySqlList.add(nullDirectionNode);
            fc = new RelFieldCollation(fc.getFieldIndex(), fc.getDirection(),
                RelFieldCollation.NullDirection.UNSPECIFIED);
          }
        }
        orderBySqlList.add(context.toSql(fc));
      }
    }
    final SqlNodeList orderByList = new SqlNodeList(orderBySqlList, SqlParserPos.ZERO);

    final SqlLiteral rowsPerMatch = e.isAllRows()
        ? SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS.symbol(POS)
        : SqlMatchRecognize.RowsPerMatchOption.ONE_ROW.symbol(POS);

    final SqlNode after;
    if (e.getAfter() instanceof RexLiteral) {
      SqlMatchRecognize.AfterOption value = (SqlMatchRecognize.AfterOption)
          ((RexLiteral) e.getAfter()).getValue2();
      after = SqlLiteral.createSymbol(value, POS);
    } else {
      RexCall call = (RexCall) e.getAfter();
      String operand = requireNonNull(stringValue(call.getOperands().get(0)),
          () -> "non-null string value expected for 0th operand of AFTER call " + call);
      after = call.getOperator().createCall(POS, new SqlIdentifier(operand, POS));
    }

    RexNode rexPattern = e.getPattern();
    final SqlNode pattern = context.toSql(null, rexPattern);
    final SqlLiteral strictStart = SqlLiteral.createBoolean(e.isStrictStart(), POS);
    final SqlLiteral strictEnd = SqlLiteral.createBoolean(e.isStrictEnd(), POS);

    RexLiteral rexInterval = (RexLiteral) e.getInterval();
    SqlIntervalLiteral interval = null;
    if (rexInterval != null) {
      interval = (SqlIntervalLiteral) context.toSql(null, rexInterval);
    }

    final SqlNodeList subsetList = new SqlNodeList(POS);
    for (Map.Entry<String, SortedSet<String>> entry : e.getSubsets().entrySet()) {
      SqlNode left = new SqlIdentifier(entry.getKey(), POS);
      List<SqlNode> rhl = new ArrayList<>();
      for (String right : entry.getValue()) {
        rhl.add(new SqlIdentifier(right, POS));
      }
      subsetList.add(
          SqlStdOperatorTable.EQUALS.createCall(POS, left,
              new SqlNodeList(rhl, POS)));
    }

    final SqlNodeList measureList = new SqlNodeList(POS);
    for (Map.Entry<String, RexNode> entry : e.getMeasures().entrySet()) {
      final String alias = entry.getKey();
      final SqlNode sqlNode = context.toSql(null, entry.getValue());
      measureList.add(as(sqlNode, alias));
    }

    final SqlNodeList patternDefList = new SqlNodeList(POS);
    for (Map.Entry<String, RexNode> entry : e.getPatternDefinitions().entrySet()) {
      final String alias = entry.getKey();
      final SqlNode sqlNode = context.toSql(null, entry.getValue());
      patternDefList.add(as(sqlNode, alias));
    }

    final SqlNode matchRecognize = new SqlMatchRecognize(POS, tableRef,
        pattern, strictStart, strictEnd, patternDefList, measureList, after,
        subsetList, rowsPerMatch, partitionList, orderByList, interval);
    return result(matchRecognize, Expressions.list(Clause.FROM), e, null);
  }

  private static SqlCall as(SqlNode e, String alias) {
    return SqlStdOperatorTable.AS.createCall(POS, e,
        new SqlIdentifier(alias, POS));
  }

  public Result visit(Uncollect e) {
    final Result x = visitInput(e, 0);
    SqlNode operand =  x.asStatement();

    //As per ANSI standard, Unnest Operator only accepts array or multiset data type,
    //So in case of select node, need to extract selectList of column name,
    //Otherwise it consumes select as subquerry.

    if (x.node instanceof SqlSelect) {
      operand = ((SqlSelect) x.node).getSelectList().get(0);
    }
    final SqlNode unnestNode = SqlStdOperatorTable.UNNEST.createCall(POS, operand);
    final List<SqlNode> operands = createAsFullOperands(e.getRowType(), unnestNode,
        requireNonNull(x.neededAlias, () -> "x.neededAlias is null, node is " + x.node));
    final SqlNode asNode = SqlStdOperatorTable.AS.createCall(POS, operands);
    return result(asNode, ImmutableList.of(Clause.FROM), e, null);
  }

  public Result visit(TableFunctionScan e) {
    List<RelDataTypeField> fieldList = e.getRowType().getFieldList();
    if (fieldList == null || fieldList.size() > 1) {
      throw new RuntimeException("Table function supports only one argument");
    }
    final List<SqlNode> inputSqlNodes = new ArrayList<>();
    final int inputSize = e.getInputs().size();
    for (int i = 0; i < inputSize; i++) {
      final Result x = visitInput(e, i);
      inputSqlNodes.add(x.asStatement());
    }
    final Context context = tableFunctionScanContext(inputSqlNodes);
    SqlNode callNode = context.toSql(null, e.getCall());
    // Convert to table function call, "TABLE($function_name(xxx))"
    SqlSpecialOperator collectionTable = new SqlCollectionTableOperator("TABLE",
        SqlModality.RELATION, e.getRowType().getFieldNames().get(0));
    SqlNode tableCall = new SqlBasicCall(
        collectionTable,
        new SqlNode[]{callNode},
        SqlParserPos.ZERO);
    SqlNode select = new SqlSelect(
        SqlParserPos.ZERO, null, null, tableCall,
        null, null, null, null, null, null, null, SqlNodeList.EMPTY);
    Map<String, RelDataType> aliasesMap = new HashMap<>();
    RelDataTypeField relDataTypeField = fieldList.get(0);
    aliasesMap.put(relDataTypeField.getName(), e.getRowType());
    return result(select, ImmutableList.of(Clause.SELECT), e, aliasesMap);
  }

  /**
   * Creates operands for a full AS operator. Format SqlNode AS alias(col_1, col_2,... ,col_n).
   *
   * @param rowType Row type of the SqlNode
   * @param leftOperand SqlNode
   * @param alias alias
   */
  public List<SqlNode> createAsFullOperands(RelDataType rowType, SqlNode leftOperand,
      String alias) {
    final List<SqlNode> result = new ArrayList<>();
    result.add(leftOperand);
    result.add(new SqlIdentifier(alias, POS));
    Ord.forEach(rowType.getFieldNames(), (fieldName, i) -> {
      if (fieldName.toLowerCase(Locale.ROOT).startsWith("expr$")) {
        fieldName = "col_" + i;
      }
      result.add(new SqlIdentifier(fieldName, POS));
    });
    return result;
  }

  @Override public void addSelect(List<SqlNode> selectList, SqlNode node,
      RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    String alias = SqlValidatorUtil.getAlias(node, -1);
    final String lowerName = name.toLowerCase(Locale.ROOT);
    if (lowerName.startsWith("expr$")) {
      // Put it in ordinalMap
      ordinalMap.put(lowerName, node);
    } else if (alias == null || !alias.equals(name)) {
      node = as(node, name);
    }
    selectList.add(node);
  }

  protected void parseCorrelTable(RelNode relNode, Result x) {
    for (CorrelationId id : relNode.getVariablesSet()) {
      correlTableMap.put(id, x.qualifiedContext());
    }
  }

  /** Stack frame. */
  private static class Frame {
    private final RelNode parent;
    @SuppressWarnings("unused")
    private final int ordinalInParent;
    private final RelNode r;
    private final boolean anon;
    private final boolean ignoreClauses;
    private final ImmutableSet<? extends Clause> expectedClauses;

    Frame(RelNode parent, int ordinalInParent, RelNode r, boolean anon,
        boolean ignoreClauses, Iterable<? extends Clause> expectedClauses) {
      this.parent = requireNonNull(parent);
      this.ordinalInParent = ordinalInParent;
      this.r = requireNonNull(r);
      this.anon = anon;
      this.ignoreClauses = ignoreClauses;
      this.expectedClauses = ImmutableSet.copyOf(expectedClauses);
    }
  }

  private SqlNode updateSqlWithNode(SqlImplementor.Result result) {
    SqlSelect sqlSelect = null;
    if (result.node instanceof SqlSelect) {
      sqlSelect = (SqlSelect) result.node;
    } else {
      sqlSelect = wrapSelect(result.node);
    }
    CTERelToSqlUtil.updateSqlNode(sqlSelect);
    return sqlSelect;
  }
}

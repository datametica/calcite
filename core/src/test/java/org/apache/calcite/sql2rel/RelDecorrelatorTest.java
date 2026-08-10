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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/**
 * Tests for {@link RelDecorrelator}.
 */
public class RelDecorrelatorTest {
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null);
  }

  @Test void testGroupKeyNotInFrontWhenDecorrelate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode before = builder.scan("EMP")
        .variable(v::set)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.call(
                    SqlStdOperatorTable.PLUS,
                    builder.literal(10),
                    builder.field(v.get(), "DEPTNO"))))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .aggregate(builder.groupKey("ENAME"), builder.max(builder.field("EMPNO")))
        .build();

    final String planBefore = ""
        + "LogicalAggregate(group=[{1}], agg#0=[MAX($0)])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[=($0, +(10, $cor0.DEPTNO))])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(before, hasTree(planBefore));

    RelNode after = RelDecorrelator.decorrelateQuery(before, builder);

    final String planAfter = ""
        + "LogicalAggregate(group=[{0}], agg#0=[MAX($1)])\n"
        + "  LogicalProject(ENAME=[$1], EMPNO=[$0])\n"
        + "    LogicalJoin(condition=[=($8, $9)], joinType=[left])\n"
        + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], "
        + "SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+(10, $7)])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  @Test void testDecorrelateCorrelatedOrderByLimitToRowNumber() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = ""
        + "SELECT dname FROM  dept WHERE 2000 > (\n"
        + "SELECT emp.sal FROM  emp where dept.deptno = emp.deptno\n"
        + "ORDER BY year(hiredate), emp.sal limit 1)";
    final RelNode originalRel;
    try {
      final SqlNode parse = planner.parse(sql);
      final SqlNode validate = planner.validate(parse);
      originalRel = planner.rel(validate).rel;
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }

    final HepProgram hepProgram = HepProgram.builder()
        .addRuleCollection(
            ImmutableList.of(
                // SubQuery program rules
                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalProject(DNAME=[$1])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
        + "    LogicalFilter(condition=[>(2000, $3)])\n"
        + "      LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n"
        + "        LogicalProject(SAL=[$0])\n"
        + "          LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC], fetch=[1])\n"
        + "            LogicalProject(SAL=[$5], EXPR$1=[EXTRACT(FLAG(YEAR), $4)])\n"
        + "              LogicalFilter(condition=[=($cor0.DEPTNO, $7)])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder);
    // Verify plan
    final String planAfter = ""
        + "LogicalProject(DNAME=[$1])\n"
        + "  LogicalJoin(condition=[=($0, $4)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[>(2000, $0)])\n"
        + "      LogicalProject(SAL=[$0], DEPTNO=[$2])\n"
        + "        LogicalFilter(condition=[<=($3, 1)])\n"
        + "          LogicalProject(SAL=[$5], EXPR$1=[EXTRACT(FLAG(YEAR), $4)], DEPTNO=[$7], rn=[ROW_NUMBER() OVER (PARTITION BY $7 ORDER BY EXTRACT(FLAG(YEAR), $4) NULLS LAST, $5 NULLS LAST)])\n"
        + "            LogicalFilter(condition=[IS NOT NULL($7)])\n"
        + "              LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7272">[CALCITE-7272]
   * Subqueries cannot be decorrelated if have set op</a>. */
  @Test void testCorrelationInSetOp1() {
    final FrameworkConfig frameworkConfig = config().build();
    final RelBuilder builder = RelBuilder.create(frameworkConfig);
    final RelOptCluster cluster = builder.getCluster();
    final Planner planner = Frameworks.getPlanner(frameworkConfig);
    final String sql = "SELECT ename,\n"
        + "    (SELECT sum(c)\n"
        + "    FROM\n"
        + "        (SELECT deptno AS c\n"
        + "        FROM dept\n"
        + "        WHERE dept.deptno = emp.deptno\n"
        + "        UNION ALL\n"
        + "        SELECT 2 AS c\n"
        + "        FROM bonus\n"
        + "        WHERE bonus.job = emp.job) AS union_subquery\n"
        + "    ) AS correlated_sum\n"
        + "FROM emp\n"
        + "ORDER BY ename";

    final RelNode originalRel;
    try {
      final SqlNode parse = planner.parse(sql);
      final SqlNode validate = planner.validate(parse);
      originalRel = planner.rel(validate).rel;
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }

    final HepProgram hepProgram = HepProgram.builder()
        .addRuleCollection(
            ImmutableList.of(
                // SubQuery program rules
                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();
    final Program program =
        Programs.of(hepProgram, true,
            requireNonNull(cluster.getMetadataProvider()));
    final RelNode before =
        program.run(cluster.getPlanner(), originalRel, cluster.traitSet(),
            Collections.emptyList(), Collections.emptyList());
    final String planBefore = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(ENAME=[$1], CORRELATED_SUM=[$8])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{2, 7}])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])\n"
        + "        LogicalUnion(all=[true])\n"
        + "          LogicalProject(C=[$0])\n"
        + "            LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "              LogicalTableScan(table=[[scott, DEPT]])\n"
        + "          LogicalProject(C=[2])\n"
        + "            LogicalFilter(condition=[=($1, $cor0.JOB)])\n"
        + "              LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(before, hasTree(planBefore));

    // Decorrelate without any rules, just "purely" decorrelation algorithm on RelDecorrelator
    final RelNode after =
        RelDecorrelator.decorrelateQuery(before, builder);
    // Verify plan
    final String planAfter = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(ENAME=[$1], CORRELATED_SUM=[$10])\n"
        + "    LogicalJoin(condition=[AND(=($2, $8), =($7, $9))], joinType=[left])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalAggregate(group=[{0, 1}], EXPR$0=[SUM($2)])\n"
        + "        LogicalProject(JOB=[$0], DEPTNO=[$1], C=[$2])\n"
        + "          LogicalUnion(all=[true])\n"
        + "            LogicalProject(JOB=[$0], DEPTNO=[$1], C=[$2])\n"
        + "              LogicalJoin(condition=[IS NOT DISTINCT FROM($1, $3)], joinType=[inner])\n"
        + "                LogicalAggregate(group=[{0, 1}])\n"
        + "                  LogicalProject(JOB=[$2], DEPTNO=[$7])\n"
        + "                    LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalProject(C=[$0], DEPTNO=[$0])\n"
        + "                  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "            LogicalProject(JOB=[$0], DEPTNO=[$1], C=[$2])\n"
        + "              LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $3)], joinType=[inner])\n"
        + "                LogicalAggregate(group=[{0, 1}])\n"
        + "                  LogicalProject(JOB=[$2], DEPTNO=[$7])\n"
        + "                    LogicalTableScan(table=[[scott, EMP]])\n"
        + "                LogicalProject(C=[2], JOB=[$1])\n"
        + "                  LogicalFilter(condition=[IS NOT NULL($1)])\n"
        + "                    LogicalTableScan(table=[[scott, BONUS]])\n";
    assertThat(after, hasTree(planAfter));
  }
}

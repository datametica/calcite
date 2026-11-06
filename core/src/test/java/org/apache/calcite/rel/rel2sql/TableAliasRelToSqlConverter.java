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

import org.apache.calcite.plan.TableAliasTrait;
import org.apache.calcite.plan.TableAliasTraitDef;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

/**
 * Test converter that writes a scan with a {@link TableAliasTrait} as {@code table AS alias},
 * the way Raven's own converter does. The plain converter drops the alias.
 *
 * <p>The class is public because the reflective visitor dispatch can only call public methods
 * of a public class.
 */
public class TableAliasRelToSqlConverter extends RelToSqlConverter {
  public TableAliasRelToSqlConverter(SqlDialect dialect) {
    super(dialect);
  }

  @Override public Result visit(TableScan e) {
    final Result result = super.visit(e);
    final TableAliasTrait aliasTrait = e.getTraitSet().getTrait(TableAliasTraitDef.instance);
    if (aliasTrait == null) {
      return result;
    }
    final SqlNode aliased =
        SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, result.node,
            new SqlIdentifier(aliasTrait.getTableAlias(), SqlParserPos.ZERO));
    return result(aliased, ImmutableList.of(Clause.FROM), e, null);
  }
}

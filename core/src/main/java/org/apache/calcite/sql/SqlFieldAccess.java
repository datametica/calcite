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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

/**
 * A <code>SqlFieldAccess</code> is a list of {@link SqlNode}s
 * occurring in a Field Access operation. It is also a
 * {@link SqlNode}, so may appear in a parse tree.
 *
 * @see SqlNode#toList()
 */
public class SqlFieldAccess extends SqlNodeList {

  public SqlFieldAccess(SqlParserPos pos) {
    super(pos);
  }

  public SqlFieldAccess(
      Collection<? extends @Nullable SqlNode> collection,
      SqlParserPos pos) {
    super(collection, pos);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
    for (SqlNode node : getList()) {
      writer.sep(".");
      node.unparse(writer, leftPrec, rightPrec);
      writer.setNeedWhitespace(true);
    }
    writer.endList(frame);
  }
}

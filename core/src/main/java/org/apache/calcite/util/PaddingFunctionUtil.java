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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;

/**
 * Handle rpad and ldap formatting
 */
public class PaddingFunctionUtil {

  private static final String OPTIONAL_VALUE = "' '";

  private PaddingFunctionUtil() {
  }

  public static void unparseCall(String functionName, SqlWriter writer, SqlCall call,
                                 int leftPrec, int rightPrec) {
    SqlWriter.Frame paddingFrame = writer.startFunCall(functionName);
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.sep(", " + OPTIONAL_VALUE);
    writer.endFunCall(paddingFrame);
  }
}
// End PaddingFunctionUtil.java

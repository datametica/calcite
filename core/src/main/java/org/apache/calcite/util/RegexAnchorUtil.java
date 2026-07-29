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

/**
 * Anchors a regular expression so that a target dialect whose predicate is a
 * <em>substring</em> matcher (BigQuery {@code REGEXP_CONTAINS}, Spark {@code rlike})
 * reproduces the <em>whole-string</em> semantics of source predicates such as
 * Teradata {@code REGEXP_SIMILAR}.
 *
 * <p>The transform is idempotent, so it may be applied to a pattern that another
 * unparse step has already anchored.
 */
public class RegexAnchorUtil {

  private RegexAnchorUtil() {
  }

  /**
   * Returns {@code regex} anchored for a whole-string match.
   *
   * <p>Rules, in order:
   * <ol>
   * <li>already anchored at both ends -&gt; returned unchanged (idempotence);</li>
   * <li>an edge {@code ^}/{@code $} counts only when <em>unescaped</em>, so
   *     {@code end\$} still gains real anchors;</li>
   * <li>a <em>top-level</em> {@code |} is wrapped in a non-capturing group first,
   *     because {@code ^a|b$} means {@code ^a} OR {@code b$} rather than a whole-string
   *     match;</li>
   * <li>otherwise only the missing side is added.</li>
   * </ol>
   */
  public static String anchorForWholeStringMatch(String regex) {
    if (regex == null) {
      return null;
    }
    boolean startAnchored = regex.startsWith("^");
    boolean endAnchored = endsWithUnescapedDollar(regex);
    if (startAnchored && endAnchored) {
      return regex;
    }
    if (hasTopLevelAlternation(regex)) {
      // The existing edge anchors, if any, become per-branch once grouped, so both
      // anchors are (re)applied around the group.
      return "^(?:" + regex + ")$";
    }
    return (startAnchored ? "" : "^") + regex + (endAnchored ? "" : "$");
  }

  /**
   * Returns whether {@code regex} ends with a {@code $} that is an anchor rather than
   * an escaped literal dollar sign.
   */
  private static boolean endsWithUnescapedDollar(String regex) {
    if (!regex.endsWith("$")) {
      return false;
    }
    int backslashes = 0;
    for (int i = regex.length() - 2; i >= 0 && regex.charAt(i) == '\\'; i--) {
      backslashes++;
    }
    return backslashes % 2 == 0;
  }

  /**
   * Returns whether {@code regex} contains an alternation {@code |} at nesting depth
   * zero, ignoring escaped characters and the contents of character classes.
   */
  private static boolean hasTopLevelAlternation(String regex) {
    int depth = 0;
    boolean inCharClass = false;
    for (int i = 0; i < regex.length(); i++) {
      char c = regex.charAt(i);
      if (c == '\\') {
        i++;
        continue;
      }
      if (inCharClass) {
        if (c == ']') {
          inCharClass = false;
        }
        continue;
      }
      switch (c) {
      case '[':
        inCharClass = true;
        break;
      case '(':
        depth++;
        break;
      case ')':
        depth--;
        break;
      case '|':
        if (depth == 0) {
          return true;
        }
        break;
      default:
        break;
      }
    }
    return false;
  }
}

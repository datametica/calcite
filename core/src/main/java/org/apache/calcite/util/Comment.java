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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a SQL comment with its text, anchor position, and type.
 *
 * <p>This class encapsulates the details of a comment that can be attached
 * to a SQL node, including the comment text, its anchor position
 * (left or right of the node), and the type of comment
 * (single-line or multi-line).</p>
 */
public class Comment {

  private static final Pattern UUID_PATTERN =
      Pattern.compile(
          "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
              + "[0-9a-fA-F]{4}-[0-9a-fA-F]{12})");

  /** The text of the comment. */
  private final String comment;

  /** The anchor position of the comment (left or right of the SQL node). */
  private final AnchorType anchorType;

  /** The type of the comment (single-line or multi-line). */
  private final CommentType commentType;

  /**
   * Constructs a Comment instance with the specified text, anchor position,
   * and type.
   *
   * @param comment the text of the comment
   * @param anchorType the anchor position of the comment
   * @param commentType the type of the comment
   */
  public Comment(String comment, AnchorType anchorType, CommentType commentType) {
    this.comment = comment;
    this.anchorType = anchorType;
    this.commentType = commentType;
  }

  /** Returns the text of the comment without its UUID prefix. */
  public String getComment() {
    return comment.replaceFirst("^[0-9a-fA-F\\-]{36}", "");
  }

  /** Returns the anchor position of the comment. */
  public AnchorType getAnchorType() {
    return anchorType;
  }

  /** Returns the type of the comment. */
  public CommentType getCommentType() {
    return commentType;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Comment other = (Comment) o;
    return Objects.equals(extractUuid(comment), extractUuid(other.comment));
  }

  @Override public int hashCode() {
    return Objects.hash(extractUuid(comment));
  }

  @Nullable
  private static String extractUuid(String input) {
    Matcher matcher = UUID_PATTERN.matcher(input);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  @Override public String toString() {
    return "Comment{"
        + "comment='" + comment + '\''
        + ", anchorType=" + anchorType
        + ", commentType=" + commentType
        + '}';
  }
}

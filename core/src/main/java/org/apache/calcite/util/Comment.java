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
 * Represents a SQL comment with its text, anchor position, and type.
 *
 *
 * <p>This class encapsulates the details of a comment that can be attached to a SQL node,
 * including the comment text, its anchor position (left or right of the node),
 * and the type of comment (single-line or multi-line).
 * </p>
 */
public class Comment {
  /**
   * The text of the comment.
   */
  String comment;
  /**
   * The anchor position of the comment (left or right of the SQL node).
   */
  AnchorToken anchorToken;
  /**
   * The type of the comment (single-line or multi-line).
   */
  CommentType commentType;

  /**
   * Constructs a Comment instance with the specified text, anchor position, and type.
   *
   * @param comment     the text of the comment
   * @param anchorToken the anchor position of the comment
   * @param commentType the type of the comment
   */
  public Comment(String comment, AnchorToken anchorToken, CommentType commentType) {
    this.comment = comment;
    this.anchorToken = anchorToken;
    this.commentType = commentType;
  }

  /**
   * Returns the text of the comment.
   *
   * @return the comment text
   */
  public String getComment() {
    return comment;
  }

  /**
   * Sets the text of the comment.
   *
   * @param comment the new comment text
   */
  public void setComment(String comment) {
    this.comment = comment;
  }

  /**
   * Returns the anchor position of the comment.
   *
   * @return the anchor position
   */
  public AnchorToken getAnchorToken() {
    return anchorToken;
  }

  /**
   * Sets the anchor position of the comment.
   *
   * @param anchorToken the new anchor position
   */
  public void setAnchorToken(AnchorToken anchorToken) {
    this.anchorToken = anchorToken;
  }

  /**
   * Returns the type of the comment.
   *
   * @return the comment type
   */
  public CommentType getCommentType() {
    return commentType;
  }

  /**
   * Sets the type of the comment.
   *
   * @param commentType the new comment type
   */
  public void setCommentType(CommentType commentType) {
    this.commentType = commentType;
  }
}

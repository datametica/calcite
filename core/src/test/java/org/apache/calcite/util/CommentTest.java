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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link Comment}.
 */
class CommentTest {

  @Test void testGetCommentRemovesUuidPrefix() {
    String uuid = "123e4567-e89b-12d3-a456-426614174000";
    String text = uuid + "/* comment text */";
    Comment comment = new Comment(text, AnchorType.LEFT, CommentType.MULTIPLE);

    assertEquals("/* comment text */", comment.getComment());
  }

  @Test void testGetCommentWithoutUuidRemainsUnchanged() {
    String text = "/* regular comment */";
    Comment comment = new Comment(text, AnchorType.RIGHT, CommentType.SINGLE);

    assertEquals(text, comment.getComment());
  }

  @Test void testEqualsTrueWhenSameUuid() {
    String uuid = "123e4567-e89b-12d3-a456-426614174000";
    Comment c1 = new Comment(uuid + "/* A */", AnchorType.LEFT, CommentType.MULTIPLE);
    Comment c2 = new Comment(uuid + "/* B */", AnchorType.RIGHT, CommentType.SINGLE);

    assertEquals(c1, c2);
    assertEquals(c1.hashCode(), c2.hashCode());
  }

  @Test void testEqualsFalseWhenDifferentUuid() {
    Comment c1 =
        new Comment("11111111-1111-1111-1111-111111111111/* X */", AnchorType.LEFT, CommentType.SINGLE);
    Comment c2 =
        new Comment("22222222-2222-2222-2222-222222222222/* X */", AnchorType.LEFT, CommentType.SINGLE);

    assertNotEquals(c1, c2);
  }

  @Test void testAnchorAndCommentTypeAccessors() {
    Comment c = new Comment("/* hello */", AnchorType.RIGHT, CommentType.SINGLE);
    assertEquals(AnchorType.RIGHT, c.getAnchorType());
    assertEquals(CommentType.SINGLE, c.getCommentType());
  }

  @Test void testToStringContainsFields() {
    Comment c = new Comment("/* note */", AnchorType.LEFT, CommentType.MULTIPLE);
    String s = c.toString();

    assertTrue(s.contains("comment='/* note */'"));
    assertTrue(s.contains("anchorType=LEFT"));
    assertTrue(s.contains("commentType=MULTIPLE"));
  }
}

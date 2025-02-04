// This is adapted/ ported from the levenshtein distance
// implementation in commons-text, which like this library is licensed
// under Apache-2.0.

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.socrata.soql.analyzer2

private[analyzer2] object Levenshtein {
  def apply(a: String, b: String): Int = {
    if (a.length == 0) {
      return b.length
    }
    if (b.length == 0) {
      return a.length
    }
    val (left, right) = if(a.length <= b.length) {
      (a, b)
    } else {
      (b, a)
    }
    val n = left.length
    val m = right.length

    val p = new Array[Int](n + 1);
    for(i <- 0 until p.length) {
      p(i) = i
    }

    for(j <- 1 to m) {
      var upperLeft = p(0);
      val rightJ = right.charAt(j - 1);
      p(0) = j;

      for(i <- 1 to n) {
        val upper = p(i);
        val cost = if(left.charAt(i - 1) == rightJ) 0 else 1;
        // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
        p(i) = Math.min(Math.min(p(i - 1) + 1, p(i) + 1), upperLeft + cost);
        upperLeft = upper;
      }
    }
    return p(n);
  }
}

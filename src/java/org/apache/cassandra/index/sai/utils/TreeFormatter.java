/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.utils;

import java.util.function.Function;

/**
 * Pretty prints heterogenous tree structures like this:
 * <pre>
 * root
 *   ├─ child 1
 *   │   ├─ child 1a
 *   │   └─ child 1b
 *   └─ child 2
 *       ├─ child 2a
 *       └─ child 2b
 * </pre>
 * @param <T> type of the node of the tree
 */
public class TreeFormatter<T>
{
    private final Function<T, Iterable<? extends T>> children;
    private final Function<T, String> toString;

    /**
     * Constructs a formatter that knows how to format trees of given type.
     *
     * @param toString a function that returns the text describing each tree node
     * @param children a function that returns a list of children nodes
     */
    public TreeFormatter(Function<T, String> toString, Function<T, Iterable<? extends T>> children)
    {
        this.children = children;
        this.toString = toString;
    }

    /**
     * Returns a multiline String with a formatted tree
     * @param root root node of the tree
     */
    public String format(T root)
    {
        StringBuilder sb = new StringBuilder();
        append(root, sb, new StringBuilder(), true, false);
        return sb.toString();
    }

    /**
     * Traverses the tree depth first and prints the tree.
     * Called once per each node.
     */
    private void append(T node, StringBuilder sb, StringBuilder padding, boolean isRoot, boolean hasRightSibling)
    {
        int origPaddingLength = padding.length();
        if (!isRoot)
        {
            sb.append(padding);
            sb.append(hasRightSibling ? " ├─ " : " └─ ");
            padding.append(hasRightSibling ? " │  " : "    ");

        }
        sb.append(toString.apply(node));
        sb.append('\n');

        var iter = children.apply(node).iterator();
        while (iter.hasNext())
        {
            T child = iter.next();
            append(child, sb, padding, false, iter.hasNext());
        }
        padding.setLength(origPaddingLength);
    }
}

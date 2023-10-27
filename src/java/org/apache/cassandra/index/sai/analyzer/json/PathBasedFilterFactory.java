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

package org.apache.cassandra.index.sai.analyzer.json;

import com.fasterxml.jackson.core.filter.TokenFilter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

class PathBasedFilterFactory {
    private final static Pattern COMMA_SEPARATOR = Pattern.compile(",");

    private final static Pattern DOT_SEPARATOR = Pattern.compile("\\.");

    // Accessed from same package, tests
    final static TokenFilter EMPTY_DOC_FILTER = IncludeNothingFilter.instance;

    /**
     * Main factory method for constructing {@link TokenFilter} for including
     * contents under given paths. Paths are passed as comma-separated list
     * of dotted-notation paths: for example
     *<code>
     *  a.b, c.d
     *</code>
     * would include properties "b" under "a", and "d" under "c", in document like
     *<code>
     *  {
     *      "a" : {
     *          "b" : 3
     *      },
     *      "b" : false,
     *      "c" : {
     *          "d" : "x"
     *      }
     *  }
     *</code>
     */
    public static TokenFilter filterForPaths(String csPaths) {
        csPaths = csPaths.trim();
        // Empty String -> match nothing; caller needs to check
        if (csPaths.isEmpty()) {
            return EMPTY_DOC_FILTER;
        }
        return filterForPaths(Arrays.asList(COMMA_SEPARATOR.split(csPaths)));
    }

    public static TokenFilter filterForPaths(List<String> csPaths) {
        // First: build a minimal inclusion tree, needed for construction of filters
        InclusionTreeNode roots = buildInclusionTree(csPaths);
        if (roots.isEmpty()) {
            return EMPTY_DOC_FILTER;
        }
        return buildFilterFromInclusionTree(roots);
    }

    private static InclusionTreeNode buildInclusionTree(List<String> paths)
    {
        InclusionTreeNode root = new InclusionTreeNode();
        for (String path : paths) {
            path = path.trim();
            if (path.isEmpty()) {
                continue;
            }
            InclusionTreeNode curr = root;
            for (String segment : DOT_SEPARATOR.split(path)) {
                InclusionTreeNode next = curr.find(segment);
                // If next doesn't exist, create it
                if (next == null) {
                    curr = curr.add(segment);
                } else {
                    curr = next;
                    if (next.isEmpty()) {
                        // If next exists and is leaf, we're done (current path longer than existing match)
                        break;
                    }
                    // Otherwise, keep going
                }
            }
            // Make sure end of the path is marked as leaf
            curr.clear();
        }
        return root;
    }

    /**
     * Method for building a {@link TokenFilter} from a given inclusion tree.
     *
     * @param node Current tree node to build filter for
     *
     * @return Filter for the given node
     */
    private static TokenFilter buildFilterFromInclusionTree(InclusionTreeNode node) {
        if (node.isEmpty()) {
            return TokenFilter.INCLUDE_ALL;
        }
        Map<String, InclusionTreeNode> childNodes = node.getChildren();
        // Optimize single-path case
        if (childNodes.size() == 1) {
            Map.Entry<String, InclusionTreeNode> entry = childNodes.entrySet().iterator().next();
            return new PathBasedFilter.SinglePathFilter(entry.getKey(),
                                                        buildFilterFromInclusionTree(entry.getValue()));
        }
        Map<String, TokenFilter> filters = new HashMap<>();
        for (Map.Entry<String, InclusionTreeNode> entry : childNodes.entrySet()) {
            filters.put(entry.getKey(), buildFilterFromInclusionTree(entry.getValue()));
        }
        return new PathBasedFilter.MultiPathFilter(filters);
    }

    /**
     * Helper type used for creating "minimal" set of inclusion paths starting
     * from a root node. Order of creating paths doesn't matter; longer paths
     * will be pruned regardless of ordering.
     */
    static class InclusionTreeNode {
        private Map<String, InclusionTreeNode> next;

        public InclusionTreeNode() { }

        public boolean isEmpty() {
            return next == null;
        }

        public Map<String, InclusionTreeNode> getChildren() {
            return next;
        }

        public InclusionTreeNode find(String key) {
            return (next == null) ? null : next.get(key);
        }

        public InclusionTreeNode add(String key) {
            if (next == null) {
                next = new HashMap<>();
            }
            InclusionTreeNode node = new InclusionTreeNode();
            next.put(key, node);
            return node;
        }

        public void clear() {
            next = null;
        }
    }

    /**
     * Filter used to indicate that results should be empty: that is, token stream
     * with no tokens.
     */
    static class IncludeNothingFilter extends TokenFilter {
        public final static IncludeNothingFilter instance = new IncludeNothingFilter();

        public TokenFilter filterStartObject() { return null; }

        public TokenFilter filterStartArray() { return null; }

        @Override
        public TokenFilter includeProperty(String name) { return null; }

        @Override
        public TokenFilter includeElement(int index) { return null; }

        @Override
        public TokenFilter includeRootValue(int index) { return null; }

        @Override
        protected boolean _includeScalar() { return false; }
    }
}
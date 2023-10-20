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

import java.util.Map;

/**
 * Base class for {@link TokenFilter} implementations used for filtering
 * intermediate tree levels (for leaves we use "include all" filter).
 * Needs to match path going through, and exclude possible scalar values
 * (so that "a.x.y" will NOT match "a.x", but will match "a.x.y.z", for example).
 */
class PathBasedFilter extends TokenFilter {
    /**
     * Different from default implementation as we should NOT allow
     * scalar values to be included at intermediate (branch) level.
     */
    @Override
    protected boolean _includeScalar() {
        return false;
    }


    /**
     * Specialized implementation that matches just a single path through JSON Object.
     */
    static class SinglePathFilter extends PathBasedFilter {
        private final String matchedSegment;

        private final TokenFilter nextFilter;

        public SinglePathFilter(String matchedSegment, TokenFilter nextFilter) {
            this.matchedSegment = matchedSegment;
            this.nextFilter = nextFilter;
        }

        @Override
        public TokenFilter includeProperty(String property) {
            if (property.equals(matchedSegment)) {
                return nextFilter;
            }
            return null;
        }
    }

    /**
     * General implementation that matches multiple paths through JSON Object.
     */
    static class MultiPathFilter extends PathBasedFilter {
        private final Map<String, TokenFilter> nextFilters;

        public MultiPathFilter(Map<String, TokenFilter> nextFilters) {
            this.nextFilters = nextFilters;
        }

        @Override
        public TokenFilter includeProperty(String property) {
            return nextFilters.get(property);
        }
    }
}
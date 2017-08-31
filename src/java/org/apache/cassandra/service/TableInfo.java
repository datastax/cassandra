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

package org.apache.cassandra.service;

import java.util.HashMap;
import java.util.Map;

public class TableInfo
{
    public static final String HAS_VIEWS = "hasViews";
    public static final String IS_VIEW = "isView";
    public static final String WAS_INCREMENTALLY_REPAIRED = "wasIncrementallyRepaired";

    public final boolean hasViews;
    public final boolean isView;
    public final boolean wasIncrementallyRepaired;

    public TableInfo(boolean hasViews, boolean isView, boolean wasIncrementallyRepaired)
    {
        this.hasViews = hasViews;
        this.isView = isView;
        this.wasIncrementallyRepaired = wasIncrementallyRepaired;
    }

    public Map<String, String> asMap()
    {
        Map<String, String> tableInfo = new HashMap<>();
        tableInfo.put(HAS_VIEWS, new Boolean(hasViews).toString());
        tableInfo.put(IS_VIEW, new Boolean(isView).toString());
        tableInfo.put(WAS_INCREMENTALLY_REPAIRED, new Boolean(wasIncrementallyRepaired).toString());
        return tableInfo;
    }

    public static TableInfo fromMap(Map<String, String> tableInfo)
    {
        boolean hasViews = Boolean.parseBoolean(tableInfo.get(HAS_VIEWS));
        boolean isView = Boolean.parseBoolean(tableInfo.get(IS_VIEW));
        boolean hasIncrementallyRepaired = Boolean.parseBoolean(tableInfo.get(WAS_INCREMENTALLY_REPAIRED));
        return new TableInfo(hasViews, isView, hasIncrementallyRepaired);
    }

    public boolean isOrHasView()
    {
        return isView || hasViews;
    }

    public String toString()
    {
        return "TableInfo{" +
               "hasViews=" + hasViews +
               ", isView=" + isView +
               ", wasIncrementallyRepaired=" + wasIncrementallyRepaired +
               '}';
    }
}

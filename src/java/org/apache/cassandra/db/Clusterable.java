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
package org.apache.cassandra.db;

/**
 * Common class for objects that are identified by a clustering prefix, and can be thus sorted by a
 * {@link ClusteringComparator}.
 *
 * Note that clusterings can have mixed accessors (necessary because the static clustering is always of ByteBuffer
 * accessor) and thus the accessor type cannot be set here.
 */
public interface Clusterable
{
    public ClusteringPrefix<?> clustering();
}

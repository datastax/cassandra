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
package org.apache.cassandra.io.sstable;

import java.nio.file.Path;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.utils.DseLegacy;

public class CorruptSSTableException extends RuntimeException
{
    public final File file;

    public CorruptSSTableException(Throwable cause, File file)
    {
        super("Corrupted: " + file, cause);
        this.file = file;
    }

    public CorruptSSTableException(Throwable cause, String path)
    {
        this(cause, new File(PathUtils.getPath(path)));
    }

    protected CorruptSSTableException(String msg, Throwable cause, File file)
    {
        super(msg, cause);
        this.file = file;
    }

    @DseLegacy
    public CorruptSSTableException(Throwable cause, Path path)
    {
        this(cause, new File(path));
    }
}

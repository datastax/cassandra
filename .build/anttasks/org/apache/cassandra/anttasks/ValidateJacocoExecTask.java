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

package org.apache.cassandra.anttasks;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.jacoco.core.tools.ExecFileLoader;

/**
 * Validates JaCoCo .exec files in the given directory (recursively) by attempting
 * to parse them with JaCoCo's {@link ExecFileLoader}. Truncated or corrupted files
 * (e.g. from killed test runners) are removed so they don't break jacoco-merge.
 */
public class ValidateJacocoExecTask extends Task
{
    private String dir;

    public void setDir(String dir)
    {
        this.dir = dir;
    }

    @Override
    public void execute() throws BuildException
    {
        if (dir == null || dir.trim().isEmpty())
        {
            log("No directory specified for jacoco-validate-exec, skipping.", Project.MSG_VERBOSE);
            return;
        }

        File baseDir = new File(dir);
        if (!baseDir.exists() || !baseDir.isDirectory())
        {
            log("JaCoCo exec directory " + baseDir.getAbsolutePath() + " does not exist, skipping validation.", Project.MSG_INFO);
            return;
        }

        List<File> execFiles = new ArrayList<>();
        collectExecFiles(baseDir, execFiles);

        log("Validating " + execFiles.size() + " JaCoCo .exec file(s) in " + baseDir.getAbsolutePath(), Project.MSG_INFO);

        int valid = 0;
        int invalid = 0;
        for (File f : execFiles)
        {
            try
            {
                ExecFileLoader loader = new ExecFileLoader();
                loader.load(f);
                valid++;
                log("JaCoCo exec file OK: " + f.getPath() + " (" + f.length() + " bytes)", Project.MSG_DEBUG);
            }
            catch (Exception e)
            {
                invalid++;
                log("Removing corrupted JaCoCo exec file: " + f.getPath() + " (" + f.length() + " bytes) - " + e.getClass().getSimpleName() + ": " + e.getMessage(), Project.MSG_ERR);
                if (!f.delete())
                {
                    log("Failed to delete corrupted JaCoCo exec file: " + f.getPath(), Project.MSG_ERR);
                }
            }
        }

        log("JaCoCo exec validation completed: " + valid + " valid, " + invalid + " corrupted/removed.", Project.MSG_INFO);
    }

    private void collectExecFiles(File current, List<File> result)
    {
        File[] files = current.listFiles();
        if (files == null) return;
        for (File f : files)
        {
            if (f.isDirectory())
            {
                collectExecFiles(f, result);
            }
            else if (f.getName().endsWith(".exec"))
            {
                result.add(f);
            }
        }
    }
}

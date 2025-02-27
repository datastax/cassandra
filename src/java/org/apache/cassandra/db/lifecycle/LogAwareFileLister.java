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
package org.apache.cassandra.db.lifecycle;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.db.Directories.FileType;
import static org.apache.cassandra.db.Directories.OnTxnErr;

/**
 * A class for listing files in a folder.
 */
final class LogAwareFileLister implements ILogAwareFileLister
{
    private static final Logger logger = LoggerFactory.getLogger(LogAwareFileLister.class);

    @Override
    public List<File> list(Path folder, BiPredicate<File, FileType> filter, OnTxnErr onTxnErr)
    {
        try
        {
            return innerList(folder, filter, onTxnErr);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(String.format("Failed to list files in %s", folder), t);
        }
    }

    protected List<File> innerList(Path folder, BiPredicate<File, FileType> filter, OnTxnErr onTxnErr) throws Throwable
    {
        // The unfiltered result
        NavigableMap<File, Directories.FileType> files = new TreeMap<>();

        list(Files.newDirectoryStream(folder))
        .stream()
        .filter((f) -> !LogFile.isLogFile(f))
        .forEach((f) -> files.put(f, FileType.FINAL));

        // Since many file systems are not atomic, we cannot be sure we have listed a consistent disk state
        // (Linux would permit this, but for simplicity we keep our behaviour the same across platforms)
        // so we must be careful to list txn log files AFTER every other file since these files are deleted last,
        // after all other files are removed
        list(Files.newDirectoryStream(folder, '*' + LogFile.EXT))
        .stream()
        .filter(LogFile::isLogFile)
        .forEach(txnFile -> classifyFiles(folder, txnFile, onTxnErr, files));

        // Finally we apply the user filter before returning our result
        return files.entrySet().stream()
                    .filter((e) -> filter.test(e.getKey(), e.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
    }

    static List<File> list(DirectoryStream<Path> stream) throws IOException
    {
        try
        {
            return StreamSupport.stream(stream.spliterator(), false)
                                .map(File::new)
                                .filter((f) -> !f.isDirectory())
                                .collect(Collectors.toList());
        }
        finally
        {
            stream.close();
        }
    }

    /**
     * We read txn log files, if we fail we throw only if the user has specified
     * OnTxnErr.THROW, else we log an error and apply the txn log anyway
     */
    void classifyFiles(Path folder, File txnFile, OnTxnErr onTxnErr, NavigableMap<File, Directories.FileType> files)
    {
        try (LogFile txn = LogFile.make(txnFile))
        {
            readTxnLog(txn, onTxnErr);
            classifyFiles(folder, txn, onTxnErr, files);
            files.put(txnFile, FileType.TXN_LOG);
        }
    }

    void readTxnLog(LogFile txn, OnTxnErr onTxnErr)
    {
        if (!txn.verify() && onTxnErr == OnTxnErr.THROW)
            throw new LogTransaction.CorruptTransactionLogException("Some records failed verification. See earlier in log for details.", txn);
    }

    void classifyFiles(Path folder, LogFile txnFile, OnTxnErr onTxnErr, NavigableMap<File, Directories.FileType> files)
    {
        Map<LogRecord, Set<File>> oldFiles = txnFile.getFilesOfType(folder, files.navigableKeySet(), LogRecord.Type.REMOVE);
        Map<LogRecord, Set<File>> newFiles = txnFile.getFilesOfType(folder, files.navigableKeySet(), LogRecord.Type.ADD);

        if (txnFile.completed())
        { // last record present, filter regardless of disk status
            setTemporary(txnFile, oldFiles.values(), newFiles.values(), files);
            return;
        }

        if (allFilesPresent(oldFiles))
        {  // all old files present, transaction is in progress, this will filter as aborted
            setTemporary(txnFile, oldFiles.values(), newFiles.values(), files);
            return;
        }

        // some old files are missing, we expect the txn file to either also be missing or completed, so check
        // disk state again to resolve any previous races on non-atomic directory listing platforms

        // if txn file also gone, then do nothing (all temporary should be gone, we could remove them if any)
        if (!txnFile.exists())
            return;

        // otherwise read the file again to see if it is completed now
        readTxnLog(txnFile, onTxnErr);

        if (txnFile.completed())
        { // if after re-reading the txn is completed then filter accordingly
            setTemporary(txnFile, oldFiles.values(), newFiles.values(), files);
            return;
        }

        logger.error("Failed to classify files in {}\n" +
                     "Some old files are missing but the txn log is still there and not completed\n" +
                     "Files in folder:\n{}\nTxn: {}",
                     folder,
                     files.isEmpty()
                        ? "\t-"
                        : String.join("\n", files.keySet().stream().map(f -> String.format("\t%s", f)).collect(Collectors.toList())),
                     txnFile.toString(true));

        // some old files are missing and yet the txn is still there and not completed
        // something must be wrong (see comment at the top of LogTransaction requiring txn to be
        // completed before obsoleting or aborting sstables)
        throw new RuntimeException(String.format("Failed to list directory files in %s, inconsistent disk state for transaction %s",
                                                 folder,
                                                 txnFile));
    }

    /** See if all files are present */
    private static boolean allFilesPresent(Map<LogRecord, Set<File>> oldFiles)
    {
        return !oldFiles.entrySet().stream()
                        .filter((e) -> e.getKey().numFiles > e.getValue().size())
                        .findFirst().isPresent();
    }

    private void setTemporary(LogFile txnFile, Collection<Set<File>> oldFiles, Collection<Set<File>> newFiles, NavigableMap<File, Directories.FileType> files)
    {
        Collection<Set<File>> temporary = txnFile.committed() ? oldFiles : newFiles;
        temporary.stream()
                 .flatMap(Set::stream)
                 .forEach((f) -> files.put(f, FileType.TEMPORARY));
    }
}

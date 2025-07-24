/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.NoSuchPaddingException;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.crypto.LocalSystemKey;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "createsystemkey", description = "Creates a system key for sstable encryption")
public class CreateSystemKey extends NodeToolCmd
{
    @Arguments(usage = "[<algorithm> <key strength> [<filename>]", description = "<algorithm[/mode/padding]>\n" +
                                                                                 "<key strength>\n" +
                                                                                 "[<file>]\n" +
                                                                                 "Key strength not required for Hmac algorithms. <file> will be appended to the directory defined in system_key_directory.")
    private List<String> args = new ArrayList<>();

    @Option(title = "directory", name = "-d", description = "Output directory")
    private String directoryOption = null;

    @Override
    public void execute(NodeProbe probe)
    {
        if (args.size() < 2)
        {
            throw new RuntimeException("Usage: nodetool createsystemkey <algorithm> <key strength> [<file>]");
        }

        String cipherName = args.get(0);
        int keyStrength = cipherName.startsWith("Hmac") ? 0 : Integer.parseInt(args.get(1));

        Path directory = directoryOption != null ? Path.of(directoryOption) : null;
        String keyLocation = null;
        PrintStream out = probe.output().out;
        PrintStream err = probe.output().err;

        try
        {
            keyLocation = args.size() > 2 ? args.get(2) : "system_key";
            Path keyPath = LocalSystemKey.createKey(directory, keyLocation, cipherName, keyStrength);

            out.printf("Successfully created key %s%n", keyPath.toString());
        }
        catch (NoSuchAlgorithmException e)
        {
            err.printf("System key (%s %s) was not created at %s%n", cipherName, keyStrength, keyLocation);
            err.println(e.getMessage());
            err.println("Available algorithms are: AES, ARCFOUR, Blowfish, DES, DESede, HmacMD5, HmacSHA1, HmacSHA256, HmacSHA384, HmacSHA512 and RC2");
            System.exit(1);
        }
        catch (InvalidParameterException | NoSuchPaddingException | IOException e)
        {
            err.printf("System key (%s %s) was not created at %s%n", cipherName, keyStrength, keyLocation);
            err.println(e.getMessage());
            System.exit(1);
        }
    }
}
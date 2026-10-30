/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.tools.dsepreflight;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;

import org.apache.cassandra.io.util.File;

/**
 * The files and directories of a DSE node the preflight inspection reads, resolved from the options, DSE's
 * {@code cassandra.yaml} and DSE's defaults, and the configuration files read. DSE's {@code cassandra.yaml} and
 * {@code dse.yaml} hold keys this software does not have, so they are read as plain YAML maps, never through this
 * software's configuration loader.
 */
final class NodeLayout
{
    /** Where a package installation of DSE keeps its configuration. */
    static final Path PACKAGE_CASSANDRA_CONF = Path.of("/etc/dse/cassandra");
    static final Path PACKAGE_DSE_YAML = Path.of("/etc/dse/dse.yaml");
    /** The DSE installation directories looked for when neither {@code --dse-home} nor {@code DSE_HOME} is set. */
    static final List<Path> DSE_HOMES = List.of(Path.of("/opt/dse"), Path.of("/usr/share/dse"));
    /** The directory DSE keeps its data in when {@code cassandra.yaml} does not say otherwise. */
    static final Path DSE_STORAGE_DIR = Path.of("/var/lib/cassandra");
    static final String RACKDC = "cassandra-rackdc.properties";
    static final String CASSANDRA_YAML = "cassandra.yaml";
    static final String DSE_YAML = "dse.yaml";

    /** What the operator gave on the command line; null where nothing was given. */
    static final class Options
    {
        Path dseHome;
        Path confDir;
        Path cassandraYaml;
        Path dseYaml;
        Path rackdc;
        List<Path> dataDirectories = new ArrayList<>();
        Path metadataDirectory;
        Path commitLogDirectory;
        Path hintsDirectory;
        /** the environment variable {@code DSE_HOME}, read by the caller */
        String dseHomeFromEnvironment;
    }

    @Nullable
    final Path dseHome;
    final Path cassandraYaml;
    final Path dseYaml;
    final Path rackdc;
    /** the YAML of {@code cassandra.yaml}; empty when the file is empty or cannot be read (see {@link #unreadable}) */
    final Map<String, Object> cassandraConf;
    /** the YAML of {@code dse.yaml}; empty when the file is empty or cannot be read (see {@link #unreadable}) */
    final Map<String, Object> dseConf;
    /** {@code cassandra-rackdc.properties}; empty when it cannot be read */
    final Properties rackdcProperties;
    final List<File> dataDirectories;
    final File metadataDirectory;
    final File commitLogDirectory;
    final File hintsDirectory;
    /** the inputs that could not be read, with why */
    final Map<String, String> unreadable = new LinkedHashMap<>();
    /** the directories taken from DSE's defaults because {@code cassandra.yaml} does not set them */
    final List<String> defaultsAssumed = new ArrayList<>();

    NodeLayout(Options options)
    {
        dseHome = dseHome(options);
        Path confDir = options.confDir != null ? options.confDir
                                               : Files.isDirectory(PACKAGE_CASSANDRA_CONF) || dseHome == null ? PACKAGE_CASSANDRA_CONF
                                                                                                              : dseHome.resolve("resources/cassandra/conf");
        cassandraYaml = options.cassandraYaml != null ? options.cassandraYaml : confDir.resolve("cassandra.yaml");
        rackdc = options.rackdc != null ? options.rackdc : cassandraYaml.resolveSibling("cassandra-rackdc.properties");
        dseYaml = options.dseYaml != null ? options.dseYaml
                                          : Files.isRegularFile(PACKAGE_DSE_YAML) || dseHome == null ? PACKAGE_DSE_YAML
                                                                                                     : dseHome.resolve("resources/dse/conf/dse.yaml");
        cassandraConf = readYaml(CASSANDRA_YAML, cassandraYaml);
        dseConf = readYaml(DSE_YAML, dseYaml);
        rackdcProperties = readProperties(rackdc);

        List<File> data = new ArrayList<>();
        if (!options.dataDirectories.isEmpty())
        {
            options.dataDirectories.forEach(p -> data.add(new File(p)));
        }
        else
        {
            List<String> configured = stringList(cassandraConf.get("data_file_directories"));
            if (configured.isEmpty())
            {
                defaultsAssumed.add("data_file_directories: " + DSE_STORAGE_DIR.resolve("data"));
                data.add(new File(DSE_STORAGE_DIR.resolve("data")));
            }
            configured.forEach(p -> data.add(new File(p)));
        }
        dataDirectories = Collections.unmodifiableList(data);
        metadataDirectory = directory(options.metadataDirectory, "metadata_directory", "metadata");
        commitLogDirectory = directory(options.commitLogDirectory, "commitlog_directory", "commitlog");
        hintsDirectory = directory(options.hintsDirectory, "hints_directory", "hints");
    }

    @Nullable
    private static Path dseHome(Options options)
    {
        if (options.dseHome != null)
            return options.dseHome;
        if (options.dseHomeFromEnvironment != null && !options.dseHomeFromEnvironment.isEmpty())
            return Path.of(options.dseHomeFromEnvironment);
        for (Path home : DSE_HOMES)
        {
            if (Files.isDirectory(home))
                return home;
        }
        return null;
    }

    private File directory(@Nullable Path override, String key, String defaultName)
    {
        if (override != null)
            return new File(override);
        Object configured = cassandraConf.get(key);
        if (configured != null && !configured.toString().isEmpty())
            return new File(configured.toString());
        Path assumed = DSE_STORAGE_DIR.resolve(defaultName);
        defaultsAssumed.add(key + ": " + assumed);
        return new File(assumed);
    }

    private Map<String, Object> readYaml(String what, Path path)
    {
        try (InputStream in = Files.newInputStream(path))
        {
            Object loaded = new Yaml(new SafeConstructor(new LoaderOptions())).load(in);
            if (loaded == null)
                return Collections.emptyMap();
            if (!(loaded instanceof Map))
                throw new IOException("not a YAML map");
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) loaded;
            return map;
        }
        catch (IOException | RuntimeException e)
        {
            unreadable.put(what, path + " (" + reason(e) + ')');
            return Collections.emptyMap();
        }
    }

    private Properties readProperties(Path path)
    {
        Properties properties = new Properties();
        try (Reader in = Files.newBufferedReader(path, StandardCharsets.UTF_8))
        {
            properties.load(in);
        }
        catch (IOException | RuntimeException e)
        {
            unreadable.put(RACKDC, path + " (" + reason(e) + ')');
        }
        return properties;
    }

    /**
     * Why a configuration file cannot be read, as the report says it: for a YAML syntax error the problem and where it is,
     * without the excerpt of the line the YAML parser adds to its message, since the line may hold a secret (a password of
     * {@code dse.yaml}, for instance); the message of the error otherwise.
     */
    static String reason(Throwable e)
    {
        if (e instanceof MarkedYAMLException)
        {
            MarkedYAMLException yaml = (MarkedYAMLException) e;
            Mark mark = yaml.getProblemMark();
            return "not valid YAML: " + yaml.getProblem() + (mark == null ? "" : " at line " + (mark.getLine() + 1) + ", column " + (mark.getColumn() + 1));
        }
        if (e instanceof YAMLException)
            return "not valid YAML";
        return String.valueOf(e.getMessage());
    }

    /** The {@code ds_version.txt} of the DSE installation, such as {@code DSVersion=6.8.65-4a7b9eb}, as its value; null when absent. */
    @Nullable
    String installedDseVersion() throws IOException
    {
        if (dseHome == null)
            return null;
        Path file = dseHome.resolve("ds_version.txt");
        if (!Files.isRegularFile(file))
            return null;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8))
        {
            String trimmed = line.trim();
            if (trimmed.startsWith("DSVersion="))
                return trimmed.substring("DSVersion=".length()).trim();
        }
        throw new IOException(file + " has no DSVersion= line");
    }

    /** A YAML value as a list of strings: a list, or a comma-separated string; empty when absent. */
    static List<String> stringList(@Nullable Object value)
    {
        List<String> result = new ArrayList<>();
        if (value instanceof List)
        {
            for (Object element : (List<?>) value)
                if (element != null)
                    result.add(element.toString().trim());
        }
        else if (value != null)
        {
            for (String element : value.toString().split(","))
                if (!element.trim().isEmpty())
                    result.add(element.trim());
        }
        return result;
    }

    /** The value at a path of nested YAML maps, such as {@code nodesync}, {@code enabled}; null when absent. */
    @Nullable
    static Object get(Map<String, Object> yaml, String... keys)
    {
        Object current = yaml;
        for (String key : keys)
        {
            if (!(current instanceof Map))
                return null;
            current = ((Map<?, ?>) current).get(key);
        }
        return current;
    }

    /** A YAML value as a string; null when absent. */
    @Nullable
    static String getString(Map<String, Object> yaml, String... keys)
    {
        Object value = get(yaml, keys);
        return value == null ? null : value.toString();
    }

    /**
     * Whether DSE may read a YAML value as true: {@code true}, {@code yes} or {@code on} in any case, the YAML 1.1 booleans
     * DSE's loader reads even when they are quoted. A value with spaces around one of them, which DSE does not read as a
     * boolean, is counted too, so that a check that blocks on a setting errs on the side of blocking.
     */
    static boolean isTrue(@Nullable Object value)
    {
        if (value == null)
            return false;
        String text = value.toString().trim().toLowerCase(Locale.ROOT);
        return text.equals("true") || text.equals("yes") || text.equals("on");
    }

    /**
     * Whether DSE may read a YAML value as false: {@code false}, {@code no} or {@code off} in any case, as DSE reads them
     * even quoted; a value with spaces around one of them is counted too, as by {@link #isTrue}.
     */
    static boolean isFalse(@Nullable Object value)
    {
        if (value == null)
            return false;
        String text = value.toString().trim().toLowerCase(Locale.ROOT);
        return text.equals("false") || text.equals("no") || text.equals("off");
    }
}

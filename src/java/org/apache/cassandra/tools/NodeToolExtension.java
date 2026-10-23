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
package org.apache.cassandra.tools;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.tools.NodeTool.NodeToolCmdRunnable;

/**
 * Client-side hook that lets a distribution bundling Cassandra (for instance HCD) add its own commands to
 * {@code nodetool} without modifying it.
 * <p>
 * Implementations are listed, comma-separated, in the
 * {@link org.apache.cassandra.config.CassandraRelevantProperties#CUSTOM_NODETOOL_EXTENSION_CLASSES} system property
 * of the <em>nodetool</em> JVM: through {@code JVM_OPTS}, or with a {@code -D} argument on the nodetool command line
 * ({@code bin/nodetool} forwards every argument starting with {@code -D} to the JVM, which also means that no command
 * can take an argument starting with {@code -D}). Each time nodetool runs they are instantiated, in the order they are
 * listed, through their public no-arg constructor, and their commands are added to the built-in ones: they are listed
 * by {@code nodetool help}, and they are parsed, run and reported exactly like the built-in commands.
 * <p>
 * A contributed command is a class annotated with airline's {@link io.airlift.airline.Command @Command} that
 * implements {@link NodeToolCmdRunnable}. Commands that talk to the node extend {@link NodeTool.NodeToolCmd}, which
 * handles the JMX connection options and hands them a connected {@link NodeProbe}; MBeans that nodetool does not know
 * about are reached through {@link NodeProbe#getMbeanServerConn()}. Those MBeans are typically registered on the node
 * by the server-side counterpart of the extension, a {@link org.apache.cassandra.service.NodeToolServerExtension}.
 * Commands that do not extend {@link NodeTool.NodeToolCmd} must not declare global options: the ones of
 * {@link NodeTool.NodeToolCmd} ({@code -h}, {@code -p}, {@code -u}, {@code -pw}, {@code -pwf}, {@code -pp}) are the
 * global options of nodetool, and airline rejects conflicting definitions.
 * <p>
 * Names must not clash: a top-level command or group contributed by an extension must not have the name of a
 * built-in command or group, nor of one contributed by another extension, and an extension cannot add commands to
 * a built-in group. A name is not empty, has no whitespace and does not start with {@code -}.
 * <p>
 * A broken extension never takes the built-in commands down: an extension that cannot be instantiated, that is
 * listed twice, or whose commands cannot be added (clashing or invalid names, a class not annotated with
 * {@link io.airlift.airline.Command @Command}, a failure while listing its commands, command definitions airline
 * rejects such as a conflicting global option or a field whose type is missing from the classpath) is skipped with
 * a warning on the error output, and nodetool runs with the built-in commands and the other extensions. The exit
 * code of nodetool is not affected by a skipped extension, only the availability of its commands is.
 * <p>
 * This is an internal SPI: it exposes the airline annotations and {@link NodeToolCmdRunnable}, and follows them if
 * nodetool ever moves to another command-line framework.
 */
public interface NodeToolExtension
{
    /**
     * @return the top-level commands this extension adds to nodetool, in the order {@code nodetool help} should list
     * them. Each class must be annotated with {@link io.airlift.airline.Command @Command}.
     */
    default List<Class<? extends NodeToolCmdRunnable>> commands()
    {
        return Collections.emptyList();
    }

    /**
     * @return the command groups this extension adds to nodetool, that is commands invoked as
     * {@code nodetool <group> <command>} like the ones of the built-in {@code repair_admin} group.
     */
    default List<CommandGroup> commandGroups()
    {
        return Collections.emptyList();
    }

    /**
     * A named group of commands, invoked as {@code nodetool <group> <command>}.
     */
    final class CommandGroup
    {
        public final String name;
        @Nullable
        public final String description;
        /**
         * The command run by a bare {@code nodetool <group>}; when null, a bare {@code nodetool <group>} prints the
         * nodetool help, as the built-in {@code bootstrap} group does.
         */
        @Nullable
        public final Class<? extends NodeToolCmdRunnable> defaultCommand;
        public final List<Class<? extends NodeToolCmdRunnable>> commands;

        /**
         * @param name the name of the group, the first argument of the commands it holds
         * @param description the description {@code nodetool help} prints for the group, or null for none
         * @param defaultCommand the command run by a bare {@code nodetool <group>}, or null to print the nodetool help
         * @param commands the commands of the group; each class must be annotated with
         *                 {@link io.airlift.airline.Command @Command}
         */
        public CommandGroup(String name,
                            @Nullable String description,
                            @Nullable Class<? extends NodeToolCmdRunnable> defaultCommand,
                            List<Class<? extends NodeToolCmdRunnable>> commands)
        {
            this.name = Objects.requireNonNull(name, "name");
            this.description = description;
            this.defaultCommand = defaultCommand;
            this.commands = ImmutableList.copyOf(Objects.requireNonNull(commands, "commands"));
        }
    }
}

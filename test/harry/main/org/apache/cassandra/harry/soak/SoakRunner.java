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

package org.apache.cassandra.harry.soak;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.dsl.SingleOperationBuilder.IdxRelation;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.execution.DriverVisitExecutor;
import org.apache.cassandra.harry.execution.DriverVisitExecutor.IndeterminateWriteException;
import org.apache.cassandra.harry.execution.DriverVisitExecutor.RetriesExhaustedException;
import org.apache.cassandra.harry.execution.DriverVisitExecutor.ServerErrorException;
import org.apache.cassandra.harry.execution.DriverVisitExecutor.ValidationMismatchException;
import org.apache.cassandra.harry.execution.DriverVisitExecutor.VisitFailure;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.op.Operations;

/**
 * Fuzzes an externally deployed cluster over the native protocol with Harry, for as long as it is told to,
 * validating every read against the model. Run it with {@code --help} for the options.
 * <p>
 * It is a single sequential writer and reader: every visit completes, retries included, before the next one
 * starts, so with writes and reads at QUORUM every acknowledged write is visible to every later read, which is
 * what the model checks. Writes carry the visit's LTS as their timestamp and so are idempotent, which is what
 * makes retrying them safe while nodes are restarted or killed underneath the run. It never flushes, compacts or
 * repairs: whatever drives the cluster's churn does that.
 * <p>
 * The same seed and options give the same schemas and the same sequence of operations; the duration only
 * decides where that sequence stops. {@code --max-visits} stops it at an exact point instead.
 *
 * <h2>Tables and memory</h2>
 * The model keeps the whole history of a table in memory, and checks a read by replaying every operation on the
 * read's partition. Both grow with every visit, so the run moves to a fresh table, with a new schema from the same
 * seed, every {@code --rotate-every} visits. Before it does, it reads back every partition of the outgoing table
 * in full and validates it, then drops the table: the model of it is gone, so nothing could check that data
 * again, and keeping it would only grow the schema and the work of the cluster's compactions. The last table is
 * kept. Rotating on a visit count rather than on heap use keeps the run reproducible.
 * <p>
 * A visit costs about 700 bytes of heap, in the history log, the data tracker and the operation itself (measured
 * at 660 to 690 bytes over generated schemas of 2 to 8 regular columns), so the default of 100 000 visits per table
 * bounds the model at about 70 MB. Checking a read costs about 30 microseconds per operation on its partition; with
 * the default 100 partitions a partition gets about 800 operations by the time its table is rotated, so the
 * slowest checks take tens of milliseconds and the model does not come to dominate the run.
 *
 * <h2>Outcomes</h2>
 * The run ends by printing one line that starts with {@value #RESULT_PREFIX} and names the outcome, then the
 * details, and exits with the outcome's code:
 * <ul>
 *     <li>{@code HARRY-SOAK RESULT: PASSED}, exit 0: the duration elapsed and every read matched the model.</li>
 *     <li>{@code HARRY-SOAK RESULT: VALIDATION_MISMATCH}, exit 10: a read returned something other than what the
 *     model expects; never retried. The details give the table, the visit's LTS, the statement with its
 *     bindings, the expected and the actual rows, and the operations on the partition are logged before.</li>
 *     <li>{@code HARRY-SOAK RESULT: INDETERMINATE_WRITE}, exit 11: a write was not acknowledged within
 *     {@code --retry-budget}, so the model cannot know whether it was applied.</li>
 *     <li>{@code HARRY-SOAK RESULT: SERVER_ERROR}, exit 12: the server failed a statement (a server error, or a
 *     read or write failure from a replica that threw), rejected one, or sent a corrupt frame. The details give
 *     the statement and the failure reason of each replica.</li>
 *     <li>{@code HARRY-SOAK RESULT: SETUP_FAILURE}, exit 13: it could not connect, create the schema or see the
 *     indexes become queryable, or a read kept failing for the whole {@code --retry-budget}.</li>
 *     <li>{@code HARRY-SOAK RESULT: ERROR}, exit 1: a bug in the runner itself; and {@code HARRY-SOAK RESULT:
 *     USAGE}, exit 2: invalid options.</li>
 * </ul>
 * Retries are logged as {@code Retrying visit <lts> after <exception class> ...}, and progress every
 * {@code --progress-interval} as {@code Progress: ...}.
 */
public class SoakRunner
{
    private static final Logger logger = LoggerFactory.getLogger(SoakRunner.class);

    public static final String RESULT_PREFIX = "HARRY-SOAK RESULT: ";

    private static final int POPULATION = 1000;
    private static final float READ_CHANCE = 0.3f;
    private static final double UNSET_CHANCE = 0.5d;
    // Writes and queries draw cell values from a small set, so that queries on regular and static columns match
    private static final int UNIQUE_CELL_VALUES = 16;
    private static final float INDEX_CHANCE = 0.7f;

    public enum Outcome
    {
        PASSED(0),
        ERROR(1),
        USAGE(2),
        VALIDATION_MISMATCH(10),
        INDETERMINATE_WRITE(11),
        SERVER_ERROR(12),
        SETUP_FAILURE(13);

        public final int exitCode;

        Outcome(int exitCode)
        {
            this.exitCode = exitCode;
        }
    }

    private final Config config;
    private final DriverVisitExecutor.Stats stats = new DriverVisitExecutor.Stats();
    private final long startNanos = System.nanoTime();

    private long visitsOnPreviousTables;
    private HistoryBuilder history;
    private SchemaSpec schema;
    private int tables;
    private long nextProgressNanos;

    public SoakRunner(Config config)
    {
        this.config = config;
    }

    public static void main(String[] args)
    {
        Config config;
        try
        {
            config = Config.parse(args);
        }
        catch (IllegalArgumentException e)
        {
            System.out.println(RESULT_PREFIX + Outcome.USAGE + " exit=" + Outcome.USAGE.exitCode + ' ' + e.getMessage());
            System.out.println(Config.USAGE);
            System.out.flush();
            System.exit(Outcome.USAGE.exitCode);
            return;
        }

        if (config == null)
        {
            System.out.println(Config.USAGE);
            System.exit(0);
            return;
        }

        Outcome outcome = new SoakRunner(config).run();
        System.out.flush();
        // The driver's threads are not daemons
        System.exit(outcome.exitCode);
    }

    public Outcome run()
    {
        logger.info("Seed: {}", config.seed);
        logger.info("Configuration: {}", config);

        Cluster cluster = null;
        Throwable failure = null;
        try
        {
            cluster = buildCluster();
            Session session = connect(cluster);
            createKeyspace(cluster, session);
            runTables(cluster, session);
        }
        catch (Throwable t)
        {
            failure = t;
        }
        finally
        {
            if (cluster != null)
                close(cluster);
        }
        return report(failure);
    }

    private void runTables(Cluster cluster, Session session)
    {
        EntropySource rng = new JdkRandomEntropySource(config.seed);
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(config.keyspace,
                                                                         "s" + Long.toUnsignedString(config.seed) + "_t",
                                                                         POPULATION,
                                                                         SchemaSpec.optionsBuilder().ifNotExists(true));
        long deadlineNanos = startNanos + TimeUnit.SECONDS.toNanos(config.durationSeconds);
        nextProgressNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.progressIntervalSeconds);

        while (true)
        {
            schema = schemaGen.generate(rng);
            tables++;
            TableWorkload workload = createTable(cluster, session, schema, rng);
            EntropySource pageSizeRng = new JdkRandomEntropySource(rng.next());
            history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                  hb -> DriverVisitExecutor.builder()
                                                                           .writeConsistencyLevel(config.writeConsistencyLevel)
                                                                           .readConsistencyLevel(config.readConsistencyLevel)
                                                                           .pageSizeSelector(visit -> 1 << pageSizeRng.nextInt(0, 13))
                                                                           .retryBudget(retryBudget())
                                                                           .stats(stats)
                                                                           .build(schema, hb, session));

            boolean done = false;
            while (history.size() < config.rotateEvery)
            {
                if (System.nanoTime() - deadlineNanos >= 0 || visits() >= config.maxVisits)
                {
                    done = true;
                    break;
                }
                workload.step(history, rng);
                maybeReportProgress();
            }

            logger.info("Validating every partition of {}.{} after {} visits", schema.keyspace, schema.table, history.size());
            for (int partition : workload.partitions)
                history.selectPartition(partition);

            if (done)
                return;

            visitsOnPreviousTables += history.size();
            history = null;
            ddl(cluster, session, "DROP TABLE IF EXISTS " + schema.keyspace + '.' + schema.table);
        }
    }

    private TableWorkload createTable(Cluster cluster, Session session, SchemaSpec schema, EntropySource rng)
    {
        logger.info("Table {}: {}", tables, schema.compile());
        ddl(cluster, session, "DROP TABLE IF EXISTS " + schema.keyspace + '.' + schema.table);
        ddl(cluster, session, schema.compile());

        Set<String> indexes = new LinkedHashSet<>();
        if (config.sai)
        {
            List<ColumnSpec<?>> columns = new ArrayList<>(schema.clusteringKeys);
            columns.addAll(schema.regularColumns);
            columns.addAll(schema.staticColumns);
            for (ColumnSpec<?> column : columns)
            {
                if (rng.nextFloat() >= INDEX_CHANCE || indexes.size() >= config.saiMaxIndexes)
                    continue;
                String index = schema.table + '_' + column.name + "_idx";
                ddl(cluster, session, String.format("CREATE INDEX IF NOT EXISTS %s ON %s.%s (%s) USING 'sai'",
                                                    index, schema.keyspace, schema.table, column.name));
                indexes.add(index);
            }
            logger.info("Indexed {} of the {} clustering, regular and static columns: {}", indexes.size(), columns.size(), indexes);
            awaitIndexesQueryable(cluster, session, indexes);
        }
        return new TableWorkload(schema, rng, config.partitions, config.maxPartitionSize, config.sai);
    }

    private DriverVisitExecutor.RetryBudget retryBudget()
    {
        return new DriverVisitExecutor.RetryBudget(TimeUnit.SECONDS.toMillis(config.retryBudgetSeconds), 100, TimeUnit.SECONDS.toMillis(5));
    }

    private long visits()
    {
        return visitsOnPreviousTables + (history == null ? 0 : history.size());
    }

    private void maybeReportProgress()
    {
        long now = System.nanoTime();
        if (now - nextProgressNanos < 0)
            return;
        nextProgressNanos = now + TimeUnit.SECONDS.toNanos(config.progressIntervalSeconds);
        Runtime runtime = Runtime.getRuntime();
        logger.info("Progress: {}s elapsed, table {} ({}.{}) at {} visits; {} visits, {} writes, {} reads validated, {} retries; heap used {} MiB",
                    TimeUnit.NANOSECONDS.toSeconds(now - startNanos), tables, schema.keyspace, schema.table,
                    history.size(), visits(), stats.writes.get(), stats.readsValidated.get(), stats.retries.get(),
                    (runtime.totalMemory() - runtime.freeMemory()) >> 20);
    }

    private Cluster buildCluster()
    {
        List<InetAddress> contactPoints = new ArrayList<>();
        for (String contactPoint : config.contactPoints)
        {
            try
            {
                contactPoints.add(InetAddress.getByName(contactPoint));
            }
            catch (UnknownHostException e)
            {
                throw new SetupFailure("Cannot resolve contact point " + contactPoint, e);
            }
        }

        DCAwareRoundRobinPolicy.Builder loadBalancing = DCAwareRoundRobinPolicy.builder();
        if (config.dc != null)
            loadBalancing.withLocalDc(config.dc);

        Cluster.Builder builder = Cluster.builder()
                                         .addContactPoints(contactPoints)
                                         .withPort(config.port)
                                         .withoutJMXReporting()
                                         .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                                         .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30_000))
                                         .withLoadBalancingPolicy(new TokenAwarePolicy(loadBalancing.build()))
                                         .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10_000)
                                                                               .setReadTimeoutMillis(config.requestTimeoutMillis))
                                         .withCompression(config.compression);
        if (config.protocolVersion != null)
        {
            if (config.protocolVersion == ProtocolVersion.NEWEST_BETA)
                builder.allowBetaProtocolVersion();
            else
                builder.withProtocolVersion(config.protocolVersion);
        }
        if (config.username != null)
            builder.withCredentials(config.username, config.password);
        return builder.build();
    }

    private Session connect(Cluster cluster)
    {
        Session session;
        try
        {
            session = cluster.connect();
        }
        catch (DriverException | IllegalStateException e)
        {
            throw new SetupFailure("Cannot connect to " + config.contactPoints + " on port " + config.port + ": " + e.getMessage(), e);
        }

        ProtocolOptions protocol = cluster.getConfiguration().getProtocolOptions();
        logger.info("Connected to cluster {} with native protocol {} and {} compression; hosts: {}",
                    cluster.getMetadata().getClusterName(), protocol.getProtocolVersion(), protocol.getCompression(),
                    cluster.getMetadata().getAllHosts());
        logClientConnections(session);
        return session;
    }

    /**
     * Logs how the server sees this client's connections, which is the evidence of the protocol version and the
     * compression that the server actually uses on them.
     */
    private void logClientConnections(Session session)
    {
        try
        {
            for (Row row : session.execute(new SimpleStatement("SELECT address, port, protocol_version, client_options FROM system_views.clients")))
            {
                Map<String, String> options = row.getMap("client_options", String.class, String.class);
                logger.info("Server sees client connection {}:{} with native protocol v{}, compression {}, driver {} {}",
                            row.getInet("address").getHostAddress(), row.getInt("port"), row.getInt("protocol_version"),
                            options.getOrDefault("COMPRESSION", "none"), options.get("DRIVER_NAME"), options.get("DRIVER_VERSION"));
            }
        }
        catch (DriverException e)
        {
            logger.warn("Could not read system_views.clients: {}", e.getMessage());
        }
    }

    private void createKeyspace(Cluster cluster, Session session)
    {
        String replication = config.dc == null
                              ? String.format("{'class': 'SimpleStrategy', 'replication_factor': %d}", config.replicationFactor)
                              : String.format("{'class': 'NetworkTopologyStrategy', '%s': %d}", config.dc, config.replicationFactor);
        ddl(cluster, session, String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s", config.keyspace, replication));
        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(config.keyspace);
        if (keyspace == null)
            throw new SetupFailure("Keyspace " + config.keyspace + " not in the driver's schema metadata after creating it");
        logger.info("Keyspace {} has replication {}", config.keyspace, keyspace.getReplication());
    }

    /**
     * Executes a schema change, retrying it as a visit's statement would be, then waits for every live node to
     * agree on the schema.
     */
    private void ddl(Cluster cluster, Session session, String cql)
    {
        CompiledStatement statement = CompiledStatement.create(cql);
        ResultSet result;
        try
        {
            result = DriverVisitExecutor.executeWithRetries(DriverVisitExecutor.NO_VISIT, statement, false, retryBudget(), stats,
                                                            () -> session.execute(new SimpleStatement(cql).setIdempotent(true)));
        }
        catch (VisitFailure e)
        {
            throw new SetupFailure("Schema change failed: " + e.getMessage(), e);
        }

        if (result.getExecutionInfo().isSchemaInAgreement())
            return;

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.setupTimeoutSeconds);
        while (!cluster.getMetadata().checkSchemaAgreement())
        {
            if (System.nanoTime() - deadline >= 0)
                throw new SetupFailure("No schema agreement within " + config.setupTimeoutSeconds + "s after: " + cql);
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    /**
     * Waits until every live node reports every index queryable in {@code system_views.sai_column_indexes}.
     */
    private void awaitIndexesQueryable(Cluster cluster, Session session, Set<String> indexes)
    {
        if (indexes.isEmpty())
            return;

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.setupTimeoutSeconds);
        while (true)
        {
            List<String> pending = new ArrayList<>();
            int checked = 0;
            for (Host host : cluster.getMetadata().getAllHosts())
            {
                if (!host.isUp())
                    continue;
                checked++;
                Set<String> queryable = new HashSet<>();
                try
                {
                    SimpleStatement query = new SimpleStatement("SELECT index_name, is_queryable FROM system_views.sai_column_indexes WHERE keyspace_name = ?",
                                                                config.keyspace);
                    for (Row row : session.execute(query.setHost(host)))
                    {
                        if (row.getBool("is_queryable"))
                            queryable.add(row.getString("index_name"));
                    }
                }
                catch (DriverException e)
                {
                    pending.add(host.getEndPoint() + ": " + e.getMessage());
                    continue;
                }
                for (String index : indexes)
                {
                    if (!queryable.contains(index))
                        pending.add(host.getEndPoint() + ": " + index);
                }
            }

            if (checked == 0)
                pending.add("no node is up");
            if (pending.isEmpty())
                return;
            if (System.nanoTime() - deadline >= 0)
                throw new SetupFailure("Indexes not queryable within " + config.setupTimeoutSeconds + "s: " + pending);
            logger.info("Waiting for indexes to become queryable: {}", pending);
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    private static void close(Cluster cluster)
    {
        try
        {
            cluster.closeAsync().get(30, TimeUnit.SECONDS);
        }
        catch (TimeoutException e)
        {
            logger.warn("Driver did not shut down within 30s");
        }
        catch (Exception e)
        {
            logger.warn("Driver shut down with an error", e);
        }
    }

    private Outcome report(Throwable failure)
    {
        Outcome outcome = outcomeOf(failure);
        StringBuilder line = new StringBuilder(RESULT_PREFIX).append(outcome)
                                                             .append(" exit=").append(outcome.exitCode)
                                                             .append(" seed=").append(config.seed);
        if (schema != null)
            line.append(" table=").append(schema.keyspace).append('.').append(schema.table);
        if (failure instanceof VisitFailure)
            line.append(" lts=").append(((VisitFailure) failure).lts);
        line.append(" visits=").append(visits())
            .append(" writes=").append(stats.writes.get())
            .append(" reads_validated=").append(stats.readsValidated.get())
            .append(" retries=").append(stats.retries.get())
            .append(" tables=").append(tables)
            .append(" elapsed_s=").append(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));

        System.out.println(line);
        if (failure != null)
        {
            System.out.println("Error: " + failure.getMessage());
            if (failure instanceof VisitFailure)
            {
                VisitFailure visitFailure = (VisitFailure) failure;
                System.out.println("Statement: " + visitFailure.statement);
                if (failure instanceof ValidationMismatchException)
                    System.out.println("Mismatch: " + visitFailure.getCause());
                if (visitFailure.lts != DriverVisitExecutor.NO_VISIT)
                    System.out.println("Reproduce: the same options with --max-visits " + visits() + " end the run with this visit");
            }
            if (schema != null)
                System.out.println("Schema: " + schema.compile());
            System.out.flush();
            logger.error("Run ended with {}", outcome, failure);
        }
        System.out.flush();
        return outcome;
    }

    private static Outcome outcomeOf(Throwable failure)
    {
        if (failure == null)
            return Outcome.PASSED;
        if (failure instanceof ValidationMismatchException)
            return Outcome.VALIDATION_MISMATCH;
        if (failure instanceof IndeterminateWriteException)
            return Outcome.INDETERMINATE_WRITE;
        if (failure instanceof ServerErrorException)
            return Outcome.SERVER_ERROR;
        if (failure instanceof SetupFailure || failure instanceof RetriesExhaustedException)
            return Outcome.SETUP_FAILURE;
        return Outcome.ERROR;
    }

    private static class SetupFailure extends RuntimeException
    {
        SetupFailure(String message)
        {
            super(message);
        }

        SetupFailure(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    /**
     * Generates the operations on one table: one write per step, and a validating read after some of them.
     */
    static class TableWorkload
    {
        final SchemaSpec schema;
        final List<Integer> partitions;
        final boolean sai;
        private final Generator<Integer> pkGen;
        private final Generator<Integer> ckGen;
        private final Set<Integer> eqOnlyClusteringColumns = new HashSet<>();
        private final Set<Integer> eqOnlyRegularColumns = new HashSet<>();
        private final Set<Integer> eqOnlyStaticColumns = new HashSet<>();

        TableWorkload(SchemaSpec schema, EntropySource rng, int partitionCount, int maxPartitionSize, boolean sai)
        {
            this.schema = schema;
            this.sai = sai;

            Generator<Integer> partitionGen = Generators.int32(0, schema.valueGenerators.pkPopulation());
            Set<Integer> partitions = new LinkedHashSet<>();
            for (int attempt = 0; partitions.size() < partitionCount && attempt < partitionCount * 10; attempt++)
                partitions.add(partitionGen.generate(rng));
            this.partitions = List.copyOf(partitions);
            this.pkGen = Generators.pick(this.partitions);
            this.ckGen = Generators.int32(0, Math.min(maxPartitionSize, schema.valueGenerators.ckPopulation()));

            // Storage-attached indexes on these types only support equality
            if (sai)
            {
                for (int i = 0; i < schema.clusteringKeys.size(); i++)
                {
                    if (CreateIndexDDL.isSAIEqOnlyType(schema.clusteringKeys.get(i).type.asServerType().unwrap()))
                        eqOnlyClusteringColumns.add(i);
                }
                for (int i = 0; i < schema.regularColumns.size(); i++)
                {
                    if (CreateIndexDDL.isSAIEqOnlyType(schema.regularColumns.get(i).type.asServerType()))
                        eqOnlyRegularColumns.add(i);
                }
                for (int i = 0; i < schema.staticColumns.size(); i++)
                {
                    if (CreateIndexDDL.isSAIEqOnlyType(schema.staticColumns.get(i).type.asServerType()))
                        eqOnlyStaticColumns.add(i);
                }
            }
        }

        void step(HistoryBuilder history, EntropySource rng)
        {
            write(history, rng);
            if (rng.nextFloat() < READ_CHANCE)
                read(history, rng);
        }

        private void write(HistoryBuilder history, EntropySource rng)
        {
            int pd = pkGen.generate(rng);
            int roll = rng.nextInt(1000);
            if (roll < 600)
                history.insert(pd, ckGen.generate(rng), regularValues(rng, UNSET_CHANCE), staticValues(rng, UNSET_CHANCE));
            else if (roll < 750)
                history.update(pd, ckGen.generate(rng), regularValues(rng, 0), staticValues(rng, 0));
            else if (roll < 820)
                history.deleteRow(pd, ckGen.generate(rng));
            else if (roll < 890)
                HistoryBuilderHelper.deleteRandomColumns(schema, pd, ckGen.generate(rng), rng, history);
            else if (roll < 995)
            {
                int row1 = ckGen.generate(rng);
                int row2 = ckGen.generate(rng);
                history.deleteRowRange(pd, Math.min(row1, row2), Math.max(row1, row2),
                                       rng.nextInt(schema.clusteringKeys.size()), rng.nextBoolean(), rng.nextBoolean());
            }
            else
                history.deletePartition(pd);
        }

        private void read(HistoryBuilder history, EntropySource rng)
        {
            int pd = pkGen.generate(rng);
            if (rng.nextInt(100) < (sai ? 40 : 15))
            {
                IdxRelation[] ckRelations = HistoryBuilderHelper.generateClusteringRelations(rng, schema.clusteringKeys.size(), ckGen, eqOnlyClusteringColumns)
                                                                .toArray(new IdxRelation[0]);
                IdxRelation[] regularRelations = HistoryBuilderHelper.generateValueRelations(rng, schema.regularColumns.size(),
                                                                                             column -> Math.min(schema.valueGenerators.regularPopulation(column), UNIQUE_CELL_VALUES),
                                                                                             eqOnlyRegularColumns::contains)
                                                                     .toArray(new IdxRelation[0]);
                IdxRelation[] staticRelations = HistoryBuilderHelper.generateValueRelations(rng, schema.staticColumns.size(),
                                                                                            column -> Math.min(schema.valueGenerators.staticPopulation(column), UNIQUE_CELL_VALUES),
                                                                                            eqOnlyStaticColumns::contains)
                                                                    .toArray(new IdxRelation[0]);
                history.select(pd, ckRelations, regularRelations, staticRelations);
                return;
            }

            // Not the one-sided slices of the DSL, as SelectHelper and DeleteHelper cannot compile them yet
            int roll = rng.nextInt(100);
            if (roll < 30)
                history.selectPartition(pd);
            else if (roll < 45)
                history.selectPartition(pd, Operations.ClusteringOrderBy.DESC);
            else if (roll < 70)
                history.selectRow(pd, ckGen.generate(rng));
            else
            {
                int row1 = ckGen.generate(rng);
                int row2 = ckGen.generate(rng);
                history.selectRowRange(pd, Math.min(row1, row2), Math.max(row1, row2),
                                       rng.nextInt(schema.clusteringKeys.size()), rng.nextBoolean(), rng.nextBoolean());
            }
        }

        private int[] regularValues(EntropySource rng, double unsetChance)
        {
            int[] values = new int[schema.regularColumns.size()];
            for (int i = 0; i < values.length; i++)
                values[i] = value(rng, schema.valueGenerators.regularPopulation(i), unsetChance);
            return values;
        }

        private int[] staticValues(EntropySource rng, double unsetChance)
        {
            int[] values = new int[schema.staticColumns.size()];
            for (int i = 0; i < values.length; i++)
                values[i] = value(rng, schema.valueGenerators.staticPopulation(i), unsetChance);
            return values;
        }

        private static int value(EntropySource rng, int population, double unsetChance)
        {
            if (rng.nextDouble() < unsetChance)
                return MagicConstants.UNSET_IDX;
            return rng.nextInt(Math.min(population, UNIQUE_CELL_VALUES));
        }
    }

    public static class Config
    {
        static final String USAGE =
        "Usage: SoakRunner --duration <time> [options]\n" +
        "  --contact-points <host,...>   contact points (default 127.0.0.1)\n" +
        "  --port <port>                 native transport port (default 9042)\n" +
        "  --dc <name>                   local datacenter; the keyspace then uses NetworkTopologyStrategy in it\n" +
        "                                (default: SimpleStrategy, and the driver picks the local datacenter)\n" +
        "  --username <name>             user to authenticate as, with --password-file\n" +
        "  --password-file <path>        file whose first line is the password\n" +
        "  --seed <long>                 seed of the schemas and operations (default: random, logged)\n" +
        "  --duration <time>             how long to run: seconds, or a number with an s, m, h or d suffix\n" +
        "  --max-visits <n>              stop after this many visits instead, if sooner\n" +
        "  --keyspace <name>             keyspace to create the tables in (default harry_soak)\n" +
        "  --replication-factor <n>      (default 3)\n" +
        "  --write-cl <level>            consistency level of writes (default QUORUM)\n" +
        "  --read-cl <level>             consistency level of reads (default QUORUM)\n" +
        "  --sai <on|off>                index columns with storage-attached indexes, and query them (default off)\n" +
        "  --sai-max-indexes <n>         most indexes on a table (default 10, the default guardrail)\n" +
        "  --compression <lz4|none>      native protocol compression (default lz4)\n" +
        "  --protocol-version <auto|n>   native protocol version; auto negotiates the newest that both support,\n" +
        "                                and a beta version is allowed explicitly (default auto)\n" +
        "  --rotate-every <n>            visits on a table before moving to a fresh one (default 100000)\n" +
        "  --partitions <n>              partitions per table (default 100)\n" +
        "  --max-partition-size <n>      rows per partition at most (default 500)\n" +
        "  --retry-budget <time>         how long a statement is retried for (default 300s)\n" +
        "  --request-timeout-ms <ms>     driver's per-request timeout (default 30000)\n" +
        "  --setup-timeout <time>        wait for schema agreement and indexes at most this long (default 600s)\n" +
        "  --progress-interval <time>    (default 60s)\n" +
        "  --help";

        private static final long UNBOUNDED_SECONDS = Long.MAX_VALUE / TimeUnit.SECONDS.toNanos(1);

        List<String> contactPoints = List.of("127.0.0.1");
        int port = 9042;
        String dc;
        String username;
        String password;
        long seed = System.nanoTime();
        long durationSeconds = -1;
        long maxVisits = Long.MAX_VALUE;
        String keyspace = "harry_soak";
        int replicationFactor = 3;
        ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;
        ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
        boolean sai = false;
        int saiMaxIndexes = 10;
        ProtocolOptions.Compression compression = ProtocolOptions.Compression.LZ4;
        ProtocolVersion protocolVersion;
        int rotateEvery = 100_000;
        int partitions = 100;
        int maxPartitionSize = 500;
        long retryBudgetSeconds = 300;
        int requestTimeoutMillis = 30_000;
        long setupTimeoutSeconds = 600;
        long progressIntervalSeconds = 60;

        /**
         * @return the configuration, or null if only the usage was asked for
         * @throws IllegalArgumentException if the arguments are invalid
         */
        public static Config parse(String... args)
        {
            Config config = new Config();
            boolean hasDuration = false;
            for (int i = 0; i < args.length; i++)
            {
                String arg = args[i];
                if (arg.equals("--help") || arg.equals("-h"))
                    return null;
                if (!arg.startsWith("--"))
                    throw new IllegalArgumentException("Unexpected argument: " + arg);

                String name = arg;
                String value;
                int eq = arg.indexOf('=');
                if (eq >= 0)
                {
                    name = arg.substring(0, eq);
                    value = arg.substring(eq + 1);
                }
                else if (name.equals("--sai") && (i + 1 == args.length || args[i + 1].startsWith("--")))
                {
                    value = "on";
                }
                else
                {
                    if (i + 1 == args.length)
                        throw new IllegalArgumentException("No value for " + name);
                    value = args[++i];
                }

                switch (name)
                {
                    case "--contact-points":
                        config.contactPoints = List.of(value.split(","));
                        break;
                    case "--port":
                        config.port = parseInt(name, value, 1);
                        break;
                    case "--dc":
                        config.dc = value;
                        break;
                    case "--username":
                        config.username = value;
                        break;
                    case "--password-file":
                        config.password = readPassword(value);
                        break;
                    case "--seed":
                        config.seed = parseLong(name, value, Long.MIN_VALUE);
                        break;
                    case "--duration":
                        config.durationSeconds = parseSeconds(name, value);
                        hasDuration = true;
                        break;
                    case "--max-visits":
                        config.maxVisits = parseLong(name, value, 1);
                        break;
                    case "--keyspace":
                        config.keyspace = value;
                        break;
                    case "--replication-factor":
                        config.replicationFactor = parseInt(name, value, 1);
                        break;
                    case "--write-cl":
                        config.writeConsistencyLevel = parseConsistencyLevel(name, value);
                        break;
                    case "--read-cl":
                        config.readConsistencyLevel = parseConsistencyLevel(name, value);
                        break;
                    case "--sai":
                        config.sai = parseOnOff(name, value);
                        break;
                    case "--sai-max-indexes":
                        config.saiMaxIndexes = parseInt(name, value, 0);
                        break;
                    case "--compression":
                        config.compression = parseCompression(name, value);
                        break;
                    case "--protocol-version":
                        config.protocolVersion = value.equals("auto") ? null : parseProtocolVersion(name, value);
                        break;
                    case "--rotate-every":
                        config.rotateEvery = parseInt(name, value, 1);
                        break;
                    case "--partitions":
                        config.partitions = parseInt(name, value, 1);
                        break;
                    case "--max-partition-size":
                        config.maxPartitionSize = parseInt(name, value, 1);
                        break;
                    case "--retry-budget":
                        config.retryBudgetSeconds = parseSeconds(name, value);
                        break;
                    case "--request-timeout-ms":
                        config.requestTimeoutMillis = parseInt(name, value, 1);
                        break;
                    case "--setup-timeout":
                        config.setupTimeoutSeconds = parseSeconds(name, value);
                        break;
                    case "--progress-interval":
                        config.progressIntervalSeconds = parseSeconds(name, value);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown option: " + name);
                }
            }

            if (!hasDuration)
            {
                if (config.maxVisits == Long.MAX_VALUE)
                    throw new IllegalArgumentException("No --duration");
                config.durationSeconds = UNBOUNDED_SECONDS;
            }
            if (config.password != null && config.username == null)
                throw new IllegalArgumentException("--password-file without --username");
            if (config.username != null && config.password == null)
                throw new IllegalArgumentException("--username without --password-file");
            return config;
        }

        private static int parseInt(String name, String value, int min)
        {
            long parsed = parseLong(name, value, min);
            if (parsed > Integer.MAX_VALUE)
                throw new IllegalArgumentException(name + " must be at most " + Integer.MAX_VALUE + ": " + value);
            return (int) parsed;
        }

        private static long parseLong(String name, String value, long min)
        {
            long parsed;
            try
            {
                parsed = Long.parseLong(value);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException(name + " must be an integer: " + value);
            }
            if (parsed < min)
                throw new IllegalArgumentException(name + " must be at least " + min + ": " + value);
            return parsed;
        }

        static long parseSeconds(String name, String value)
        {
            TimeUnit unit = TimeUnit.SECONDS;
            String number = value;
            if (!value.isEmpty() && Character.isLetter(value.charAt(value.length() - 1)))
            {
                number = value.substring(0, value.length() - 1);
                switch (value.charAt(value.length() - 1))
                {
                    case 's': unit = TimeUnit.SECONDS; break;
                    case 'm': unit = TimeUnit.MINUTES; break;
                    case 'h': unit = TimeUnit.HOURS; break;
                    case 'd': unit = TimeUnit.DAYS; break;
                    default: throw new IllegalArgumentException(name + " must be a number of seconds, or end in s, m, h or d: " + value);
                }
            }
            long seconds = unit.toSeconds(parseLong(name, number, 0));
            if (seconds >= UNBOUNDED_SECONDS)
                throw new IllegalArgumentException(name + " is too long: " + value);
            return seconds;
        }

        private static ConsistencyLevel parseConsistencyLevel(String name, String value)
        {
            try
            {
                return ConsistencyLevel.valueOf(value.toUpperCase(Locale.ROOT));
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException(name + " must be one of " + Arrays.toString(ConsistencyLevel.values()) + ": " + value);
            }
        }

        private static boolean parseOnOff(String name, String value)
        {
            switch (value.toLowerCase(Locale.ROOT))
            {
                case "on": case "true": case "yes": return true;
                case "off": case "false": case "no": return false;
                default: throw new IllegalArgumentException(name + " must be on or off: " + value);
            }
        }

        private static ProtocolOptions.Compression parseCompression(String name, String value)
        {
            switch (value.toLowerCase(Locale.ROOT))
            {
                case "lz4": return ProtocolOptions.Compression.LZ4;
                case "none": return ProtocolOptions.Compression.NONE;
                default: throw new IllegalArgumentException(name + " must be lz4 or none: " + value);
            }
        }

        private static ProtocolVersion parseProtocolVersion(String name, String value)
        {
            try
            {
                return ProtocolVersion.fromInt(parseInt(name, value, 1));
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException(name + " must be auto or a protocol version the driver supports, " + Arrays.toString(ProtocolVersion.values()) + ": " + value);
            }
        }

        private static String readPassword(String path)
        {
            try
            {
                List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
                if (lines.isEmpty())
                    throw new IllegalArgumentException("Password file " + path + " is empty");
                return lines.get(0);
            }
            catch (IOException e)
            {
                throw new IllegalArgumentException("Cannot read password file " + path + ": " + e);
            }
        }

        @Override
        public String toString()
        {
            return "contact-points=" + String.join(",", contactPoints) +
                   " port=" + port +
                   " dc=" + (dc == null ? "(driver's choice)" : dc) +
                   " username=" + (username == null ? "(none)" : username) +
                   " seed=" + seed +
                   " duration=" + (durationSeconds == UNBOUNDED_SECONDS ? "(unbounded)" : durationSeconds + "s") +
                   " max-visits=" + (maxVisits == Long.MAX_VALUE ? "(unbounded)" : maxVisits) +
                   " keyspace=" + keyspace +
                   " replication-factor=" + replicationFactor +
                   " write-cl=" + writeConsistencyLevel +
                   " read-cl=" + readConsistencyLevel +
                   " sai=" + (sai ? "on" : "off") +
                   " sai-max-indexes=" + saiMaxIndexes +
                   " compression=" + compression +
                   " protocol-version=" + (protocolVersion == null ? "auto" : protocolVersion) +
                   " rotate-every=" + rotateEvery +
                   " partitions=" + partitions +
                   " max-partition-size=" + maxPartitionSize +
                   " retry-budget=" + retryBudgetSeconds + 's' +
                   " request-timeout-ms=" + requestTimeoutMillis +
                   " setup-timeout=" + setupTimeoutSeconds + 's' +
                   " progress-interval=" + progressIntervalSeconds + 's';
        }
    }
}

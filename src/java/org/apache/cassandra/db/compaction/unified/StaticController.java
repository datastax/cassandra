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

package org.apache.cassandra.db.compaction.unified;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.compaction.CompactionPick;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.Overlaps;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static org.apache.cassandra.db.compaction.unified.DSECompatibilityUtils.getSystemProperty;

/**
 * The static compaction controller periodically checks the IO costs
 * that result from the current configuration of the {@link UnifiedCompactionStrategy}.
 */
public class StaticController extends Controller
{
    /**
     * The scaling parameters W, one per bucket index and separated by a comma.
     * Higher indexes will use the value of the last index with a W specified.
     */
    private static final String DEFAULT_STATIC_SCALING_PARAMETERS = getSystemProperty(SCALING_PARAMETERS_OPTION, "T4");
    static final String DEFAULT_VECTOR_STATIC_SCALING_PARAMETERS = getSystemProperty(VECTOR_PREFIX + SCALING_PARAMETERS_OPTION, "L10");
    private final int[] scalingParameters;

    @VisibleForTesting // comp. simulation
    public StaticController(Environment env,
                            int[] scalingParameters,
                            double[] survivalFactors,
                            long dataSetSize,
                            long minSSTableSize,
                            long flushSizeOverride,
                            long currentFlushSize,
                            double maxSpaceOverhead,
                            int maxSSTablesToCompact,
                            long expiredSSTableCheckFrequency,
                            boolean ignoreOverlapsInExpirationCheck,
                            int baseShardCount,
                            boolean isReplicaAware,
                            long targetSStableSize,
                            double sstableGrowthModifier,
                            int reservedThreadsPerLevel,
                            Reservations.Type reservationsType,
                            Overlaps.InclusionMethod overlapInclusionMethod,
                            boolean parallelizeOutputShards,
                            boolean hasVectorType,
                            TableMetadata metadata)
    {
        super(MonotonicClock.preciseTime,
              env,
              survivalFactors,
              dataSetSize,
              minSSTableSize,
              flushSizeOverride,
              currentFlushSize,
              maxSpaceOverhead,
              maxSSTablesToCompact,
              expiredSSTableCheckFrequency,
              ignoreOverlapsInExpirationCheck,
              baseShardCount,
              isReplicaAware,
              targetSStableSize,
              sstableGrowthModifier,
              reservedThreadsPerLevel,
              reservationsType,
              overlapInclusionMethod,
              parallelizeOutputShards,
              hasVectorType,
              metadata);
        this.scalingParameters = scalingParameters;
    }

    static Controller fromOptions(Environment env,
                                  double[] survivalFactors,
                                  long dataSetSize,
                                  long minSSTableSize,
                                  long flushSizeOverride,
                                  double maxSpaceOverhead,
                                  int maxSSTablesToCompact,
                                  long expiredSSTableCheckFrequency,
                                  boolean ignoreOverlapsInExpirationCheck,
                                  int baseShardCount,
                                  boolean isReplicaAware,
                                  long targetSStableSize,
                                  double sstableGrowthModifier,
                                  int reservedThreadsPerLevel,
                                  Reservations.Type reservationsType,
                                  Overlaps.InclusionMethod overlapInclusionMethod,
                                  boolean parallelizeOutputShards,
                                  boolean hasVectorType,
                                  TableMetadata metadata,
                                  Map<String, String> options,
                                  boolean useVectorOptions)
    {
        int[] scalingParameters;
        if (options.containsKey(STATIC_SCALING_FACTORS_OPTION))
            scalingParameters = parseScalingParameters(options.get(STATIC_SCALING_FACTORS_OPTION));
        else
            scalingParameters = parseScalingParameters(options.getOrDefault(SCALING_PARAMETERS_OPTION,
                                                                            useVectorOptions ? DEFAULT_VECTOR_STATIC_SCALING_PARAMETERS
                                                                                             : DEFAULT_STATIC_SCALING_PARAMETERS));

        long currentFlushSize = flushSizeOverride;

        File f = getControllerConfigPath(metadata);
        try
        {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader(f));
            if (jsonObject.get("current_flush_size") != null && flushSizeOverride == 0)
            {
                currentFlushSize = (long) jsonObject.get("current_flush_size");
                logger.debug("Successfully read stored current_flush_size from disk");
            }
        }
        catch (IOException e)
        {
            logger.debug("No controller config file found. Using starting value instead.");
        }
        catch (ParseException e)
        {
            logger.warn("Unable to parse saved flush size. Using starting value instead:", e);
        }
        catch (FSError e)
        {
            logger.warn("Unable to read controller config file. Using starting value instead:", e);
        }
        catch (Throwable e)
        {
            logger.warn("Unable to read controller config file. Using starting value instead:", e);
            JVMStabilityInspector.inspectThrowable(e);
        }
        return new StaticController(env,
                                    scalingParameters,
                                    survivalFactors,
                                    dataSetSize,
                                    minSSTableSize,
                                    flushSizeOverride,
                                    currentFlushSize,
                                    maxSpaceOverhead,
                                    maxSSTablesToCompact,
                                    expiredSSTableCheckFrequency,
                                    ignoreOverlapsInExpirationCheck,
                                    baseShardCount,
                                    isReplicaAware,
                                    targetSStableSize,
                                    sstableGrowthModifier,
                                    reservedThreadsPerLevel,
                                    reservationsType,
                                    overlapInclusionMethod,
                                    parallelizeOutputShards,
                                    hasVectorType,
                                    metadata);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        String parameters = options.remove(SCALING_PARAMETERS_OPTION);
        if (parameters != null)
            parseScalingParameters(parameters);
        String factors = options.remove(STATIC_SCALING_FACTORS_OPTION);
        if (factors != null)
            parseScalingParameters(factors);
        if (parameters != null && factors != null)
            throw new ConfigurationException(String.format("Either '%s' or '%s' should be used, not both", SCALING_PARAMETERS_OPTION, STATIC_SCALING_FACTORS_OPTION));
        return options;
    }

    @Override
    public int getScalingParameter(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < scalingParameters.length ? scalingParameters[index] : scalingParameters[scalingParameters.length - 1];
    }

    @Override
    public int getPreviousScalingParameter(int index)
    {
        //scalingParameters is not updated in StaticController so previous scalingParameters = scalingParameters
        return getScalingParameter(index);
    }

    @Override
    public boolean isRecentAdaptive(CompactionPick pick)
    {
        return false;
    }

    @Override
    public int getMaxRecentAdaptiveCompactions()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public void storeControllerConfig()
    {
        storeOptions(metadata, scalingParameters, getFlushSizeBytes());
    }

    @Override
    public String toString()
    {
        return String.format("Static controller, m: %d, o: %s, scalingParameters: %s, cost: %s", minSSTableSize,
                             Arrays.toString(survivalFactors),
                             printScalingParameters(scalingParameters),
                             calculator);
    }
}

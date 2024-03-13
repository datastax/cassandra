/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.utils.leaks.detection;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * A factory and repository of {@link LeaksDetector} instances. We need this repository because
 * we want all the detectors in one place so that we can implement nodetool commands that update the
 * detectors' parameters.
 */
public class LeaksDetectorFactory implements LeaksDetectionMXBean
{
    private final static LeaksDetectorFactory instance = createInstance();

    private final CopyOnWriteArrayList<LeaksDetector> detectors = new CopyOnWriteArrayList<>();

    private static LeaksDetectorFactory createInstance()
    {
        LeaksDetectorFactory ret = new LeaksDetectorFactory();
        registerJMX(ret);
        return ret;
    }

    /**
     * Register the singleton instance as an mxBean.
     */
    private static void registerJMX(LeaksDetectorFactory factory)
    {
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

            ObjectName me = new ObjectName(LEAKS_DETECTION_MBEAN_NAME);
            if (!mbs.isRegistered(me))
                mbs.registerMBean(factory, new ObjectName(LEAKS_DETECTION_MBEAN_NAME));
        }
        catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a leaks detector with the default parameters specified in the yaml. If no parameters
     * have been specified in the yaml, then create one using the default parameters defined
     * in {@link LeaksDetectionParams}.
     */
    public static <T> LeaksDetector<T> create(String name, Class cls, LeakLevel level)
    {
        return create(name, cls, LeaksDetectionParams.DEFAULT_SAMPLING_PROBABILITY, level);
    }

    /**
     * Create a leaks detector with the default parameters specified in the yaml. If no parameters
     * have been specified in the yaml, then create one using the given default sampling probability and
     * the remaining default parameters defined in {@link LeaksDetectionParams}.
     */
    public static <T> LeaksDetector<T> create(String name, Class cls, double defaultSamplingProb, LeakLevel level)
    {
        return create(name, cls, DatabaseDescriptor.getLeaksDetectionParams(defaultSamplingProb), level);
    }

    /**
     * Create a leaks detector with the given leaks detection parameters. This bypasses yaml
     * configuration and default parameters defined in {@link LeaksDetectionParams}.
     */
    public static <T> LeaksDetector<T> create(String name, Class cls, LeaksDetectionParams params, LeakLevel level)
    {
        return create(LeaksResource.create(name, cls, level), params);
    }

    private static <T> LeaksDetector<T> create(LeaksResource resource, LeaksDetectionParams params)
    {
        return instance.add(resource, params);
    }

    /**
     * Add a new detector and return it.
     */
    @VisibleForTesting
    <T> LeaksDetector<T> add(LeaksResource resource, LeaksDetectionParams params)
    {
        LeaksDetector<T> ret = new LeaksDetectorImpl<>(resource, params);
        detectors.add(ret);
        return ret;
    }

    @Override
    public List<String> getRegisteredDetectors()
    {
        return detectors.stream()
                        .map(detector -> String.format("%-"+ LeaksResource.MAX_NAME_LENGTH + "s - %s", detector.getResource(), detector.getParams()))
                        .collect(Collectors.toList());
    }

    @Override
    public void setSamplingProbability(String resourceName, double samplingProbability)
    {
        setProperty(resourceName, params ->params.withSamplingProbability(samplingProbability));
    }

    @Override
    public void setSamplingProbability(double samplingProbability)
    {
        setProperty(params ->params.withSamplingProbability(samplingProbability));
    }

    @Override
    public void setMaxStacksCacheSizeMb(String resourceName, int maxStacksCacheSizeMb)
    {
        setProperty(resourceName, params ->params.withMaxStacksCacheSizeMb(maxStacksCacheSizeMb));
    }

    @Override
    public void setMaxStacksCacheSizeMb(int maxStacksCacheSizeMb)
    {
        setProperty(params ->params.withMaxStacksCacheSizeMb(maxStacksCacheSizeMb));
    }

    @Override
    public void setNumAccessRecords(String resourceName, int numAccessRecords)
    {
        setProperty(resourceName, params ->params.withNumAccessRecords(numAccessRecords));
    }

    @Override
    public void setNumAccessRecords(int numAccessRecords)
    {
        setProperty(params ->params.withNumAccessRecords(numAccessRecords));
    }

    @Override
    public void setMaxStackDepth(String resourceName, int maxStackDepth)
    {
        setProperty(resourceName, params ->params.withMaxStackDepth(maxStackDepth));
    }

    @Override
    public void setMaxStackDepth(int maxStackDepth)
    {
        setProperty(params ->params.withMaxStackDepth(maxStackDepth));
    }

    private void setProperty(String resourceName, Function<LeaksDetectionParams, LeaksDetectionParams> paramsProvider)
    {
        try
        {
            boolean found = false;
            for (LeaksDetector detector : detectors)
            {
                if (detector.getResource().name.equals(resourceName))
                {
                    if (!found)
                        found = true;
                    detector.setParams(paramsProvider.apply(detector.getParams()));
                }
            }

            if (!found)
                throw new IllegalStateException(String.format("No detector found for resource %s", resourceName));
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(t);
        }
    }

    private void setProperty(Function<LeaksDetectionParams, LeaksDetectionParams> paramsProvider)
    {
        for (LeaksDetector detector : detectors)
            detector.setParams(paramsProvider.apply(detector.getParams()));
    }
}

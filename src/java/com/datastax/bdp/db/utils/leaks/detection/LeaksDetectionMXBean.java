/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.utils.leaks.detection;

import java.util.List;

/**
 * An interface for customizing or retrieving leaks detection parameters. Refer to {@link LeaksDetectionParams} for
 * a description of the parameters.
 */
public interface LeaksDetectionMXBean
{
    /**
     * The name of the mbean that will be registered by {@link LeaksDetectorFactory}.
     */
    String LEAKS_DETECTION_MBEAN_NAME = "com.datastax.bdp.db.utils.leaks.detection:type=LeaksDetection,name=LeaksDetection";

    /**
     * @return the registered leak detectors, each of them described in a string formatted by {@link LeaksDetectionParams}.
     */
    List<String> getRegisteredDetectors();

    /**
     * Set the sampling probability for a specific resource detector.
     */
    void setSamplingProbability(String resourceName, double samplingProbability);

    /**
     * Set the sampling probability for all detectors.
     */
    void setSamplingProbability(double samplingProbability);

    /**
     * Set the max stacks cache size for a specific resource detector.
     */
    void setMaxStacksCacheSizeMb(String resourceName, int maxStacksCacheSizeMb);

    /**
     * Set the max stacks cache size for all detectors.
     */
    void setMaxStacksCacheSizeMb(int maxStacksCacheSizeMb);

    /**
     * Set the max number of access records for a specific resource detector.
     */
    void setNumAccessRecords(String resourceName, int numAccessRecords);

    /**
     * Set the max number of access records for all detectors.
     */
    void setNumAccessRecords(int numAccessRecords);

    /**
     * Set the max stack depth for a specific resource detector.
     */
    void setMaxStackDepth(String resourceName, int maxStackDepth);

    /**
     * Set the max stack depth for all detectors.
     */
    void setMaxStackDepth(int maxStackDepth);
}

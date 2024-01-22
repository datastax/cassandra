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

package org.apache.cassandra.sensors.read;

import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.Type;

/**
 * Increment {@link Type#READ_BYTES} {@link Sensor}s by the data size of each iterated row and sync the sensor values
 * when the iterator is closed.
 */
public class TrackingRowIterator extends Transformation<UnfilteredRowIterator>
{
    private final RequestTracker requestTracker;

    public TrackingRowIterator()
    {
        this.requestTracker = RequestTracker.instance;
    }

    @Override
    public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator iter)
    {
        return Transformation.apply(iter, this);
    }

    @Override
    public Row applyToStatic(Row row)
    {
        // TODO: Not worth tracking the static row?
        return row;
    }

    @Override
    public Row applyToRow(Row row)
    {
        RequestSensors sensors = requestTracker.get();
        if (sensors != null && row.isRow())
            sensors.incrementSensor(Type.READ_BYTES, row.dataSize());

        return row;
    }

    @Override
    protected void onClose()
    {
        super.onClose();

        RequestSensors sensors = requestTracker.get();
        if (sensors != null)
            sensors.syncAllSensors();
    }
}

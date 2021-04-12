/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.db.marshal;

import org.apache.cassandra.db.marshal.geometry.GeometricType;
import org.apache.cassandra.db.marshal.geometry.Point;

public class PointType extends AbstractGeometricType<Point>
{
    public static final PointType instance = new PointType();

    public PointType()
    {
        super(GeometricType.POINT);
    }
}

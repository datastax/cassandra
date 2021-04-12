/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.db.marshal;

import org.apache.cassandra.db.marshal.geometry.GeometricType;
import org.apache.cassandra.db.marshal.geometry.Polygon;

public class PolygonType extends AbstractGeometricType<Polygon>
{
    public static final PolygonType instance = new PolygonType();

    public PolygonType()
    {
        super(GeometricType.POLYGON);
    }
}

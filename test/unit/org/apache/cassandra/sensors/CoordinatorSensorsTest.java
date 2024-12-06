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

package org.apache.cassandra.sensors;

import java.util.Optional;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;

public class CoordinatorSensorsTest extends CQLTester
{
    @BeforeClass
    public static void setupClass()
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        // a workaround to force sensors registry to initialize (i.e. subscribe to SchemaChangeListener) before the
        // test creates the keyspace and tables
        SensorsRegistry.instance.clear();
    }

    @Test
    public void testReadSensors()
    {
        createTable("create table %s (pk int, ck int, v text, primary key(pk, ck))");
        Context context = Context.from(currentTableMetadata());
        Optional<Sensor> memorySensor = SensorsRegistry.instance.getSensor(context, Type.MEMORY_BYTES);
        assertThat(memorySensor).isEmpty();

        executeNet("insert into %s (pk, ck, v) values (1, 1, 'v1')");
        executeNet("select * from %s where pk = 1");
        memorySensor = SensorsRegistry.instance.getSensor(context, Type.MEMORY_BYTES);
        assertThat(memorySensor).isPresent();
        double memoryBytes = memorySensor.get().getValue();
        assertThat(memoryBytes).isGreaterThan(0);

        executeNet("select * from %s where pk = 1");
        assertThat(memorySensor.get().getValue()).isEqualTo(memoryBytes * 2);
    }

    @Test
    public void testRangeReadSensors()
    {
        createTable("create table %s (pk int, ck int, v text, primary key(pk, ck))");
        Context context = Context.from(currentTableMetadata());
        Optional<Sensor> memorySensor = SensorsRegistry.instance.getSensor(context, Type.MEMORY_BYTES);
        assertThat(memorySensor).isEmpty();

        executeNet("insert into %s (pk, ck, v) values (1, 1, 'v1')");
        executeNet("insert into %s (pk, ck, v) values (1, 2, 'v2')");
        executeNet("insert into %s (pk, ck, v) values (1, 3, 'v3')");
        executeNet("select * from %s");
        memorySensor = SensorsRegistry.instance.getSensor(context, Type.MEMORY_BYTES);
        assertThat(memorySensor).isPresent();
        double memoryBytes = memorySensor.get().getValue();
        assertThat(memoryBytes).isGreaterThan(0);

        executeNet("select * from %s");
        assertThat(memorySensor.get().getValue()).isEqualTo(memoryBytes * 2);
    }
}

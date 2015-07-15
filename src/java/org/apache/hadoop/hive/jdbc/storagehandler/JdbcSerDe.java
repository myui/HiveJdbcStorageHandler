/*
 * Copyright 2013-2015 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.jdbc.storagehandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

/**
 * Serialize/Deserialize a tuple.
 */
public class JdbcSerDe implements SerDe {
    private static final Log LOG = LogFactory.getLog(JdbcSerDe.class);

    private DbRecordWritable cachedWritable;

    private int fieldCount;

    private StructObjectInspector objectInspector;
    private List<Object> deserializeCache;

    public JdbcSerDe() {}

    @Override
    public void initialize(Configuration sysConf, Properties tblProps) throws SerDeException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("tblProps: " + tblProps);
        }

        String columnNameProperty = tblProps.getProperty(Constants.LIST_COLUMNS);
        String columnTypeProperty = tblProps.getProperty(Constants.LIST_COLUMN_TYPES);

        List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
        String[] columnTypes = columnTypeProperty.split(":");
        assert (columnTypes.length == columnNames.size()) : "columnNames: " + columnNames
                + ", columnTypes: " + Arrays.toString(columnTypes);

        int[] types = HiveJdbcBridgeUtils.hiveTypesToSqlTypes(columnTypes);
        this.cachedWritable = new DbRecordWritable(types);
        this.fieldCount = types.length;

        final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(columnTypes.length);
        for(int i = 0; i < types.length; i++) {
            ObjectInspector oi = HiveJdbcBridgeUtils.getObjectInspector(types[i], columnTypes[i]);
            fieldOIs.add(oi);
        }
        this.objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, fieldOIs);
        this.deserializeCache = new ArrayList<Object>(columnTypes.length);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return DbRecordWritable.class;
    }

    /**
     * This method takes an object representing a row of data from Hive, and uses
     * the ObjectInspector to get the data for each column and serialize.
     */
    @Override
    public DbRecordWritable serialize(Object row, ObjectInspector inspector) throws SerDeException {
        final StructObjectInspector structInspector = (StructObjectInspector) inspector;
        final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
        if(fields.size() != fieldCount) {
            throw new SerDeException(String.format("Required %d columns, received %d.", fieldCount, fields.size()));
        }

        cachedWritable.clear();

        for(int i = 0; i < fieldCount; i++) {
            StructField structField = fields.get(i);
            if(structField != null) {
                Object field = structInspector.getStructFieldData(row, structField);
                ObjectInspector fieldOI = structField.getFieldObjectInspector();
                Object javaObject = HiveJdbcBridgeUtils.deparseObject(field, fieldOI);
                cachedWritable.set(i, javaObject);
            }
        }

        return cachedWritable;
    }

    /**
     * This method does the work of deserializing a record into Java objects that
     * Hive can work with via the ObjectInspector interface.
     */
    @Override
    public Object deserialize(Writable record) throws SerDeException {
        if(!(record instanceof DbRecordWritable)) {
            throw new SerDeException("Expected DbTupleWritable, received "
                    + record.getClass().getName());
        }
        DbRecordWritable tuple = (DbRecordWritable) record;
        deserializeCache.clear();

        for(int i = 0; i < fieldCount; i++) {
            Object o = tuple.get(i);
            deserializeCache.add(o);
        }

        return deserializeCache;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // TODO Auto-generated method stub
        return null;
    }
}

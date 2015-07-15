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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class HiveJdbcBridgeUtils {

    public static int[] hiveTypesToSqlTypes(String[] hiveTypes) throws SerDeException {
        final int[] result = new int[hiveTypes.length];
        for(int i = 0; i < hiveTypes.length; i++) {
            result[i] = hiveTypeToSqlType(hiveTypes[i]);
        }
        return result;
    }

    /**
     * @see org.apache.hadoop.hive.jdbc.Utils#hiveTypeToSqlType(String)
     */
    private static int hiveTypeToSqlType(String hiveType) throws SerDeException {
        final String lctype = hiveType.toLowerCase();
        if("string".equals(lctype)) {
            return Types.VARCHAR;
        } else if("float".equals(lctype)) {
            return Types.FLOAT;
        } else if("double".equals(lctype)) {
            return Types.DOUBLE;
        } else if("boolean".equals(lctype)) {
            return Types.BOOLEAN;
        } else if("tinyint".equals(lctype)) {
            return Types.TINYINT;
        } else if("smallint".equals(lctype)) {
            return Types.SMALLINT;
        } else if("int".equals(lctype)) {
            return Types.INTEGER;
        } else if("bigint".equals(lctype)) {
            return Types.BIGINT;
        } else if("timestamp".equals(lctype)) {
            return Types.TIMESTAMP;
        } else if("binary".equals(lctype)) {
            return Types.BINARY;
        } else if(lctype.startsWith("array<")) {
            return Types.ARRAY;
        }
        throw new SerDeException("Unrecognized column type: " + hiveType);
    }

    public static ObjectInspector getObjectInspector(int sqlType, String hiveType)
            throws SerDeException {
        switch(sqlType) {
            case Types.VARCHAR:
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            case Types.FLOAT:
                return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
            case Types.DOUBLE:
                return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            case Types.BOOLEAN:
                return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
            case Types.TINYINT:
                return PrimitiveObjectInspectorFactory.javaByteObjectInspector;
            case Types.SMALLINT:
                return PrimitiveObjectInspectorFactory.javaShortObjectInspector;
            case Types.INTEGER:
                return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
            case Types.BIGINT:
                return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
            case Types.TIMESTAMP:
                return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
            case Types.BINARY:
                return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
            case Types.ARRAY:
                String hiveElemType = hiveType.substring(hiveType.indexOf('<') + 1, hiveType.indexOf('>')).trim();
                int sqlElemType = hiveTypeToSqlType(hiveElemType);
                ObjectInspector listElementOI = getObjectInspector(sqlElemType, hiveElemType);
                return ObjectInspectorFactory.getStandardListObjectInspector(listElementOI);
            default:
                throw new SerDeException("Cannot find getObjectInspecto for: " + hiveType);
        }
    }

    public static Object deparseObject(Object field, ObjectInspector fieldOI) throws SerDeException {
        switch(fieldOI.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector oi = (PrimitiveObjectInspector) fieldOI;
                return oi.getPrimitiveJavaObject(field);
            }
            case LIST: {
                ListObjectInspector listOI = (ListObjectInspector) fieldOI;
                List<?> elements = listOI.getList(field);
                List<Object> list = new ArrayList<Object>(elements.size());
                ObjectInspector elemOI = listOI.getListElementObjectInspector();
                for(Object elem : elements) {
                    Object o = deparseObject(elem, elemOI);
                    list.add(o);
                }
                return list;
            }
            default:
                throw new SerDeException("Unexpected fieldOI: " + fieldOI);
        }
    }

    public static Object readObject(DataInput in, int sqlType) throws IOException {
        switch(sqlType) {
            case Types.VARCHAR:
                return in.readUTF();
            case Types.FLOAT:
                return Float.valueOf(in.readFloat());
            case Types.DOUBLE:
                return Double.valueOf(in.readDouble());
            case Types.BOOLEAN:
                return Boolean.valueOf(in.readBoolean());
            case Types.TINYINT:
                return Byte.valueOf(in.readByte());
            case Types.SMALLINT:
                return Short.valueOf(in.readShort());
            case Types.INTEGER:
                return Integer.valueOf(in.readInt());
            case Types.BIGINT:
                return Long.valueOf(in.readLong());
            case Types.TIMESTAMP: {
                long time = in.readLong();
                return new Timestamp(time);
            }
            case Types.BINARY: {
                int size = in.readInt();
                byte[] b = new byte[size];
                in.readFully(b);
                return b;
            }
            case Types.ARRAY: {
                int size = in.readInt();
                if(size == 0) {
                    return Collections.emptyList();
                }
                int elemType = in.readInt();
                Object[] a = new Object[size];
                for(int i = 0; i < size; i++) {
                    Object o = readObject(in, elemType);
                    a[i] = o;
                }
                return Arrays.asList(a);
            }
            default:
                throw new IOException("Cannot read Object for type: " + sqlType);
        }
    }

    public static void writeObject(Object obj, int sqlType, DataOutput out) throws IOException {
        switch(sqlType) {
            case Types.VARCHAR: {
                String s = obj.toString();
                out.writeUTF(s);
                return;
            }
            case Types.FLOAT: {
                Float f = (Float) obj;
                out.writeFloat(f.floatValue());
                return;
            }
            case Types.DOUBLE: {
                Double d = (Double) obj;
                out.writeDouble(d.doubleValue());
                return;
            }
            case Types.BOOLEAN: {
                Boolean b = (Boolean) obj;
                out.writeBoolean(b.booleanValue());
                return;
            }
            case Types.TINYINT: {
                Byte b = (Byte) obj;
                out.writeByte(b.intValue());
                return;
            }
            case Types.SMALLINT: {
                Short s = (Short) obj;
                out.writeShort(s.shortValue());
                return;
            }
            case Types.INTEGER: {
                Integer i = (Integer) obj;
                out.writeInt(i.intValue());
                return;
            }
            case Types.BIGINT: {
                Long l = (Long) obj;
                out.writeLong(l.longValue());
                return;
            }
            case Types.TIMESTAMP: {
                Timestamp time = (Timestamp) obj;
                out.writeLong(time.getTime());
                return;
            }
            case Types.BINARY: {
                byte[] b = (byte[]) obj;
                out.writeInt(b.length);
                out.write(b);
                return;
            }
            case Types.ARRAY: {
                List<?> list = (List<?>) obj;
                int size = list.size();
                out.writeInt(size);
                if(size > 0) {
                    Object firstElem = list.get(0);
                    Class<?> clazz = firstElem.getClass();
                    int elemSqlType = toSqlType(clazz);
                    out.writeInt(elemSqlType);
                    for(Object e : list) {
                        writeObject(e, elemSqlType, out);
                    }
                }
                return;
            }
            default:
                throw new IOException("Cannot write Object '" + obj.getClass().getSimpleName()
                        + "' as type: " + sqlType);
        }
    }

    public static int toSqlType(Class<?> clazz) throws IOException {
        if(clazz == String.class) {
            return Types.VARCHAR;
        }
        if(clazz == Float.class) {
            return Types.FLOAT;
        }
        if(clazz == Double.class) {
            return Types.DOUBLE;
        }
        if(clazz == Boolean.class) {
            return Types.BOOLEAN;
        }
        if(clazz == Byte.class) {
            return Types.TINYINT;
        }
        if(clazz == Short.class) {
            return Types.SMALLINT;
        }
        if(clazz == Integer.class) {
            return Types.INTEGER;
        }
        if(clazz == Long.class) {
            return Types.BIGINT;
        }
        if(clazz == Timestamp.class) {
            return Types.TIMESTAMP;
        }
        if(clazz == byte[].class) {
            return Types.BINARY;
        }
        if(clazz.isArray()) {
            return Types.ARRAY;
        }
        throw new IOException("Cannot resolve SqlType for + " + clazz.getName());
    }
}

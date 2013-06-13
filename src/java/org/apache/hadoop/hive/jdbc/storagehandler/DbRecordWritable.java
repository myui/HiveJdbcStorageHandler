package org.apache.hadoop.hive.jdbc.storagehandler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class DbRecordWritable implements Writable, DBWritable {

    private Object[] columnValues; // primitive java Object or java.util.List
    private int[] columnTypes;

    public DbRecordWritable() {}

    public DbRecordWritable(int[] types) {
        this.columnValues = new Object[types.length];
        this.columnTypes = types;
    }

    public void clear() {
        Arrays.fill(columnValues, null);
    }

    public void set(int i, Object javaObject) {
        columnValues[i] = javaObject;
    }

    public Object get(int i) {
        return columnValues[i];
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int cols = meta.getColumnCount();
        final Object[] columns = new Object[cols];
        final int[] types = new int[cols];
        for(int i = 0; i < cols; i++) {
            Object col = rs.getObject(i + 1);
            columns[i] = col;
            if(col == null) {
                types[i] = meta.getColumnType(i + 1);
            }
        }
        this.columnValues = columns;
        this.columnTypes = types;
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        assert (columnValues != null);
        assert (columnTypes != null);
        final Object[] r = this.columnValues;
        final int cols = r.length;
        for(int i = 0; i < cols; i++) {
            final Object col = r[i];
            if(col == null) {
                statement.setNull(i + 1, columnTypes[i]);
            } else {
                statement.setObject(i + 1, col);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if(size == -1) {
            return;
        }
        if(columnValues == null) {
            this.columnValues = new Object[size];
            this.columnTypes = new int[size];
        } else {
            clear();
        }
        for(int i = 0; i < size; i++) {
            int sqlType = in.readInt();
            columnTypes[i] = sqlType;
            Object v = HiveJdbcBridgeUtils.readObject(in, sqlType);
            columnValues[i] = v;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void write(DataOutput out) throws IOException {
        if(columnValues == null) {
            out.writeInt(-1);
            return;
        }
        if(columnTypes == null) {
            out.writeInt(-1);
            return;
        }
        final Object[] values = this.columnValues;
        final int[] types = this.columnTypes;
        assert (values.length == types.length);
        out.writeInt(values.length);
        for(int i = 0; i < values.length; i++) {
            HiveJdbcBridgeUtils.writeObject(values[i], types[i], out);
        }
    }

}

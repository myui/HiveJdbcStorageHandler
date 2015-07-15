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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter;
import org.apache.hadoop.util.StringUtils;

public class JdbcRecordWriter implements RecordWriter {
    private static final Log LOG = LogFactory.getLog(JdbcRecordWriter.class);

    @SuppressWarnings("rawtypes")
    private final DBRecordWriter delegate;

    @SuppressWarnings("rawtypes")
    public JdbcRecordWriter(DBRecordWriter writer) {
        this.delegate = writer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(Writable w) throws IOException {
        delegate.write((DbRecordWritable) w, null);
    }

    @Override
    public void close(boolean abort) throws IOException {
        if(abort) {
            Connection conn = delegate.getConnection();
            try {
                conn.rollback();
            } catch (SQLException ex) {
                LOG.warn(StringUtils.stringifyException(ex));
            } finally {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    throw new IOException(ex.getMessage());
                }
            }
        } else {
            delegate.close(null);
        }
    }

}

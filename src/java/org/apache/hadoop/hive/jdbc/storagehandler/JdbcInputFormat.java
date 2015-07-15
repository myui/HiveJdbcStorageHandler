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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;

public class JdbcInputFormat extends DBInputFormat<DbRecordWritable> {

    private boolean jobConfSet = false;

    /**
     * @see org.apache.hadoop.util.ReflectionUtils#setConf(Object, Configuration)
     */
    @Override
    public void setConf(Configuration conf) {
        // delay for TableJobProperties is set to the jobConf
    }

    /**
     * @see org.apache.hadoop.hive.ql.exec.FetchOperator#getRecordReader()
     */
    @Override
    public void configure(JobConf jobConf) {
        // delay for TableJobProperties is set to the jobConf
    }

    @Override
    public RecordReader<LongWritable, DbRecordWritable> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter)
            throws IOException {
        if(!jobConfSet) {
            super.configure(jobConf);
            this.jobConfSet = true;
        }
        return super.getRecordReader(split, jobConf, reporter);
    }

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int chunks) throws IOException {
        if(!jobConfSet) {
            super.configure(jobConf);
            this.jobConfSet = true;
        }
        return super.getSplits(jobConf, chunks);
    }

}

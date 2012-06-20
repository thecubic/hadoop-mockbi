/*
 *    Copyright 2012 Dave Carlson <thecubic@thecubic.net>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.thecubic.mockbi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.record.CsvRecordInput;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.record.Index;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.RecordInput;
import org.apache.hadoop.record.RecordOutput;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;

import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

// tenant view: request vs. site, request, site
// request view: tenant vs. site, tenant, site
// site view: tenant vs. request, tenant, request

public class MockBIMapReduce {	
	public void run() throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MockBIMapReduce");
        
        Scan scan = new Scan();
        job.setJarByClass( this.getClass() );
        TableMapReduceUtil.initTableMapperJob(MockBI.logTableName, scan, MockBILogMapper.class, MockBISummaryKey.class, DoubleWritable.class, job, true);
        TableMapReduceUtil.initTableReducerJob(MockBI.summaryTableName, MockBILogReducer.class, job);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        
        conf.setInt("initialsummary_interval", Calendar.MINUTE);
        
        conf.set(TableOutputFormat.OUTPUT_TABLE, MockBI.summaryTableName);
        
        //job.setOutputKeyClass();
        //job.setOutputValueClass();
        
        TableMapReduceUtil.addDependencyJars(job);
        
        boolean jobr;
        
        try {
            jobr = job.waitForCompletion(true);
        } catch (InterruptedException e) {
            System.err.println("killing job");
            job.killJob();
            jobr = false;
        }
        
    }
	
	public static void main(String[] argv) throws Exception {
        MockBIMapReduce m = new MockBIMapReduce();
        m.run();
		
		
		
		//job.setInputFormatClass(MockBILogMapper.class);
	}
}

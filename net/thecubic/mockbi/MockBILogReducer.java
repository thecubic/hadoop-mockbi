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


/* reduces each analysis domain + bin to a number summary */
    /* MockBISummaryKey, [ latencies ] -> MockBISummaryKey, number_summary */
    public class MockBILogReducer extends TableReducer<MockBISummaryKey, DoubleWritable, ImmutableBytesWritable> {
        protected void setup(Context context) throws IOException, InterruptedException {
            
        }
                
        protected void reduce(MockBISummaryKey key, Iterable<DoubleWritable> dataPoints, Context context) throws IOException {
            RollingNumberSummary rns = new RollingNumberSummary();          
            for (DoubleWritable dataPoint: dataPoints) {
                rns.push( dataPoint.get() );
            }
            Put record;
            try {
                record = new Put( key.getBytes() );
            } catch (IOException e) {
                context.getCounter( getClass().getName(), "record.IOException").increment(1L);              
                e.printStackTrace();
                // well, i tried; i gave up
                return;
            }
            MockBIHBase mbitc = new MockBIHBase();
            
            byte[] columnFamily = key.getColumnFamily();
            mbitc.addSummaryColumn( columnFamily );
            
            record.add( columnFamily, MockBI.countKey, Integer.toString( rns.n ).getBytes() );
            record.add( columnFamily, MockBI.minKey, Double.toString( rns.min ).getBytes() );
            record.add( columnFamily, MockBI.maxKey, Double.toString( rns.max ).getBytes() );
            record.add( columnFamily, MockBI.meanKey, Double.toString( rns.mean ).getBytes() );
            record.add( columnFamily, MockBI.stddevKey, Double.toString( rns.getSD() ).getBytes() );
            try {
                context.write( new ImmutableBytesWritable( columnFamily ), record );
            } catch (IOException e) {
                context.getCounter( getClass().getName(), "context.IOException").increment(1L);
                e.printStackTrace();
            } catch (InterruptedException e) {
                context.getCounter( getClass().getName(), "context.InterruptedException").increment(1L);
                e.printStackTrace();
            }
        }
    }
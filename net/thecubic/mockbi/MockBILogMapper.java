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
import java.util.List;
import java.util.ArrayList;


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


 /* maps rows from the input table (MockBILogs) to summary key objects */    
    /* requestId, requestParams -> MockBISummaryKey, latency */
    public class MockBILogMapper extends TableMapper<MockBISummaryKey, DoubleWritable> {        
      public List<MockBISummaryKey> templates = new ArrayList<MockBISummaryKey>();
      public MockBILogMapper() {
          super();
      }
      
      protected void setup(Context context) throws IOException, InterruptedException {
          MockBIHBase mbh = new MockBIHBase();
          for (String cfn : mbh.getSummaryColumnFamilyNames()) {
              MockBISummaryKey _k = MockBISummaryKey.fromColumnFamilyName( cfn );
              if ( _k != null ) {
                  templates.add( _k );
              }
          }
      }
      protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {
          //MockBISummaryKey nkey = new MockBISummaryKey();
          
          long timestamp = Long.parseLong( new String( row.getColumnLatest(MockBI.logColumnFamily, MockBIDatum.timestamp).getValue() ) );
          String site = new String( row.getColumnLatest(MockBI.logColumnFamily, MockBIDatum.site).getValue() );
          String tenant = new String( row.getColumnLatest(MockBI.logColumnFamily, MockBIDatum.tenant).getValue() );
          String request = new String( row.getColumnLatest(MockBI.logColumnFamily, MockBIDatum.request).getValue() );
          int latency = Integer.parseInt( new String( row.getColumnLatest(MockBI.logColumnFamily, MockBIDatum.latency).getValue() ) );
          
          //nkey.setTimeSeriesBin( timestamp );
          //nkey.setBin( Calendar.MINUTE );
          
          for ( MockBISummaryKey templ: templates ) {
              MockBISummaryKey nkey = new MockBISummaryKey();
              nkey.setTimeSeriesBin( timestamp );
              nkey.setBin( templ.binLevel );
              for ( String k: templ.analysisParams.keySet() ) {
                  if ( k.equalsIgnoreCase(MockBIDatum.siteName) ) {
                      nkey.putAnalysisParams( new String(MockBIDatum.site), site );
                  } else if ( k.equalsIgnoreCase(MockBIDatum.tenantName) ) {
                      nkey.putAnalysisParams( new String(MockBIDatum.tenant), tenant );
                  } else if ( k.equalsIgnoreCase(MockBIDatum.requestName) ) {
                      nkey.putAnalysisParams( new String(MockBIDatum.request), request );
                  }
              }
              context.write( nkey, new DoubleWritable( latency ) );
              
          }
          
          
          //nkey.putAnalysisParams( new String(MockBIDatum.tenant), tenant );
          
          
          
          //context.write( nkey, new DoubleWritable( latency ) );
//                putin.add( logColumnFamily, MockBIDatum.request, MockBIDatum.genRequest() );
//    putin.add( logColumnFamily, MockBIDatum.site, MockBIDatum.genSite() );
//    putin.add( logColumnFamily, MockBIDatum.tenant, MockBIDatum.genTenant() );
//    putin.add( logColumnFamily, MockBIDatum.timestamp, MockBIDatum.genTimestamp() );    
//    putin.add( logColumnFamily, MockBIDatum.latency, MockBIDatum.genLatency() );    
          
      }
    }
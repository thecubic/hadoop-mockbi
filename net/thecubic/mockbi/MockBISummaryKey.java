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


public class MockBISummaryKey extends Record {      
        
    
        int binLevel;
        Date timeSeriesBin;     
        Long timeSeriesInterval;        
        LinkedHashMap<String,String> analysisParams;
        
        public int getBinLevel() {
            return binLevel;
        }

        public void setBinLevel(int binLevel) {
            this.binLevel = binLevel;
        }

        public Date getTimeSeriesBin() {
            return timeSeriesBin;
        }

        public void setTimeSeriesBin(Date timeSeriesBin) {
            this.timeSeriesBin = timeSeriesBin;
        }
        
        public void setTimeSeriesBin(long timestamp) {
            this.timeSeriesBin = new Date(timestamp);
        }

        public Long getTimeSeriesInterval() {
            return timeSeriesInterval;
        }

        public void setTimeSeriesInterval(Long timeSeriesInterval) {
            this.timeSeriesInterval = timeSeriesInterval;
        }

        public LinkedHashMap<String, String> getAnalysisParams() {
            return analysisParams;
        }
        
        // bare constructor, nothing to see here
        public MockBISummaryKey() {         
            analysisParams = new LinkedHashMap<String,String>();
        }
        
        // adjust the time series to the desired bin and interval
        public void setBin( int bin ) {
            Calendar cal = Calendar.getInstance();          
            cal.setTime(timeSeriesBin);         
            if ( bin == Calendar.MINUTE ) {
                cal.clear(Calendar.MILLISECOND);
                cal.clear(Calendar.SECOND);
                timeSeriesInterval = 60000L;
            } else if ( bin == Calendar.HOUR ) {
                cal.clear(Calendar.MILLISECOND);
                cal.clear(Calendar.SECOND);
                cal.clear(Calendar.MINUTE);
                timeSeriesInterval = 3600000L;
            } else if ( bin == Calendar.DATE ) {
                cal.clear(Calendar.MILLISECOND);
                cal.clear(Calendar.SECOND);
                cal.set(Calendar.HOUR_OF_DAY, 0);                               
                timeSeriesInterval = 86400000L;
            } else if ( bin == Calendar.WEEK_OF_YEAR ) {
                cal.clear(Calendar.MILLISECOND);
                cal.clear(Calendar.SECOND);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                // awkward: add negative days to get to the first day of week
                cal.add( Calendar.DATE, cal.get(Calendar.DAY_OF_WEEK) - 1 );
                timeSeriesInterval = 604800000L;
            }
            binLevel = bin;
            timeSeriesBin = cal.getTime();
        }        
        
        public void setAnalysisParams( LinkedHashMap<String,String> aP ) {
            analysisParams = aP;            
        }
        
        public void putAnalysisParams( String key, String value ) {
            //if ( analysisParams == null ) {
            //    analysisParams = new LinkedHashMap<String,String>();
            //}
            analysisParams.put(key, value);                 
        }
                
        @Override
        public boolean equals(Object arg0) {
            return compareTo(arg0) == 0;
        }
    
        @Override
        public int compareTo(Object arg0) throws ClassCastException {
            MockBISummaryKey foreign = (MockBISummaryKey) arg0;
            int bincmp = timeSeriesBin.compareTo( foreign.getTimeSeriesBin() );
            int sizecmp = timeSeriesInterval.compareTo( foreign.getTimeSeriesInterval() );
            if (sizecmp != 0) {
                return sizecmp;
            }            
            if (bincmp != 0) {
                return bincmp;
            }       
            int apcmp = new Integer( analysisParams.size() ).compareTo( foreign.analysisParams.size() );
            if (apcmp != 0) {
                return apcmp;
            }       
           return analysisParams.equals( foreign.analysisParams ) ? 0 : 1;      
        }
    
        
        @Override
        public void deserialize(RecordInput ri, String ctx) 
                throws IOException {
            ri.startRecord(ctx);            
            timeSeriesBin = new Date( ri.readLong("timeSeriesBin") * 1000L );
            timeSeriesInterval = new Long( ri.readLong("timeSeriesInterval") * 1000L );
            binLevel = ri.readInt("binLevel");
            //analysisParams = new LinkedHashMap<String,String>();
            for ( Index ad_idx = ri.startMap("analysisParams"); !ad_idx.done(); ad_idx.incr() ) {
                analysisParams.put( ri.readString("key"), ri.readString("value"));
            }
            ri.endMap("analysisParams");
            ri.endRecord(ctx);
        }

        @Override
        public void serialize(RecordOutput ro, String ctx)
                throws IOException {
            ro.startRecord(this, ctx);
            ro.writeLong( timeSeriesBin.getTime() / 1000L, "timeSeriesBin");
            ro.writeLong( timeSeriesInterval.longValue() / 1000L, "timeSeriesInterval");
            ro.writeInt( binLevel, "binLevel");         
            // HADOOP-7889
            TreeMap<String,String> tempmap = new TreeMap<String,String>();
            for (String key: analysisParams.keySet() ) {
                tempmap.put(key, analysisParams.get(key));
            }           
            ro.startMap(tempmap, "analysisParams");
            for (String key: analysisParams.keySet() ) {
                ro.writeString(key, "key");
                ro.writeString(analysisParams.get(key), "value");
            }
            ro.endMap(tempmap, "analysisParams");
            ro.endRecord(this, ctx);
        }       
        
        private String columnFamilyName = null;
        public String getColumnFamilyName() {
          if ( columnFamilyName == null ) {
            StringBuilder sb = new StringBuilder();
            sb.append("SUMMARY_");
            for ( String param : analysisParams.keySet() ) {
                sb.append( param.toUpperCase() );
                sb.append( "_" );
            }
          
            String intervalName = MockBI.intervalNameMap.get( binLevel );
            sb.append( intervalName );
          
            columnFamilyName = sb.toString();
          }
          return columnFamilyName;
        }
        
        public static MockBISummaryKey fromColumnFamilyName( String cfn ) {
            String[] components = cfn.split("_");

            if ( ! components[0].equals("SUMMARY") ) {
                return null;
            }
                        for ( String component: components ) {
                System.out.println( "COM: " + component );
            }
            Integer tsB = MockBI.nameIntervalMap.get( components[components.length-1] );
            if ( tsB == null ) {
                return null;
            }
            
            
            
            MockBISummaryKey tgt = new MockBISummaryKey();
            
            tgt.setTimeSeriesBin( 0 );
            tgt.setBin( tsB );
            
            for ( int idx = 1; idx < components.length-1 ; idx++ ) {
                tgt.putAnalysisParams( components[idx].toLowerCase(), "(null)" );
            }
            
            return tgt;
        }
        
        public byte[] getColumnFamily() {
            if (columnFamilyName == null) {
                getColumnFamilyName();
            }
            return columnFamilyName.getBytes();
        }
        
        /* byte-serialization for HBase */
        
        public byte[] getBytes() throws IOException {
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              CsvRecordOutput cro = new CsvRecordOutput(baos);
              this.serialize(cro);
              return baos.toByteArray();
        }       
        
        public void setBytes(byte[] bytes) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            CsvRecordInput cri = new CsvRecordInput(bais);
            this.deserialize(cri);
        }
        
    }
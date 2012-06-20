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
 
import java.util.List;
import java.util.Random;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;
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

public class MockBIHBase {      
      public Configuration hbase = null;
      public HBaseAdmin hadmin = null;
      public HTable logHTable = null; 
      public HTable summaryHTable = null;
      
      
      public MockBIHBase() throws IOException {
          hbase = HBaseConfiguration.create();
          hadmin = new HBaseAdmin( hbase );
          
          if ( ! hadmin.tableExists( MockBI.logTableName ) ) {
              createLogTable();
          }                   
          logHTable = new HTable( hbase, MockBI.logTableName);
          
          if ( ! hadmin.tableExists( MockBI.summaryTableName ) ) {
              createSummaryTable();
          }
          summaryHTable = new HTable ( hbase, MockBI.summaryTableName);
          
          
      }
      
      public void storeLog(Put putin) throws IOException {
          logHTable.put(putin);
      }
      public void storeLog(List<Put> putin) throws IOException {
          logHTable.put(putin);
      }
      
      public void createLogTable() throws IOException {                 
          if ( ! hadmin.tableExists(MockBI.logTableName) ) {
              HTableDescriptor htabled = new HTableDescriptor(MockBI.logTableName);
              HColumnDescriptor hcfd = new HColumnDescriptor(MockBI.logColumnFamilyName);
              hcfd.setBloomFilterType(BloomType.ROW);
              // hcfd.setCompressionType(Algorithm.SNAPPY);
              htabled.addFamily(hcfd);
              hadmin.createTable(htabled);
          }                 
      }
      
      public void destroyLogTable() throws IOException {
          hadmin.disableTable( MockBI.logTableName );
          hadmin.deleteTable( MockBI.logTableName );
      }
      
      public void createSummaryTable() throws IOException {
          if ( ! hadmin.tableExists(MockBI.summaryTableName) ) {
              HTableDescriptor htabled = new HTableDescriptor(MockBI.summaryTableName);
              //HColumnDescriptor hcfd = new HColumnDescriptor(MockBI.logColumnFamilyName);
              //hcfd.setBloomFilterType(BloomType.ROW);
              // hcfd.setCompressionType(Algorithm.SNAPPY);
              //htabled.addFamily(hcfd);
              hadmin.createTable(htabled);
          }                           
          
      }
      
      public void destroySummaryTable() throws IOException {
          hadmin.disableTable( MockBI.summaryTableName );
          hadmin.deleteTable( MockBI.summaryTableName );
      }
      
      /*public MockBIDataStorer() throws IOException {
          hbase = HBaseConfiguration.create();
          hadmin = new HBaseAdmin( hbase );       
          if ( ! hadmin.tableExists(MockBI.logTableName) ) {
              HTableDescriptor htabled = new HTableDescriptor(MockBI.logTableName);
              HColumnDescriptor hcfd = new HColumnDescriptor(MockBI.logColumnFamilyName);
              hcfd.setBloomFilterType(BloomType.ROW);
              // hcfd.setCompressionType(Algorithm.SNAPPY);
              htabled.addFamily(hcfd);
              hadmin.createTable(htabled);
          }       
          htable = new HTable( hbase, MockBI.logTableName );
      } */  
      
      
      /*public MockBITableCreator() throws IOException {
          hbase = HBaseConfiguration.create();
          hadmin = new HBaseAdmin( hbase );       
          if ( ! hadmin.tableExists(summaryTableName) ) {
              HTableDescriptor htabled = new HTableDescriptor(summaryTableName);
              //HColumnDescriptor hcfd = new HColumnDescriptor(logColumnFamilyName);
              //hcfd.setBloomFilterType(BloomType.ROW);
              // hcfd.setCompressionType(Algorithm.SNAPPY);
              //htabled.addFamily(hcfd);
              hadmin.createTable(htabled);
          }       
          htable = new HTable( hbase, summaryTableName );
      } */  
      public void addSummaryColumn( byte[] columnFamily ) throws IOException {
          if ( ! summaryHTable.getTableDescriptor().hasFamily( columnFamily ) ) {              
              hadmin.disableTable( MockBI.summaryTableName );
              HColumnDescriptor hcfd = new HColumnDescriptor( columnFamily );
              hcfd.setBloomFilterType( BloomType.ROW );
              hadmin.addColumn( MockBI.summaryTableName , hcfd );
              hadmin.enableTable( MockBI.summaryTableName );                            
          }
      }
      
      public void addSummaryColumns( List<byte[]> columnFamilies ) throws IOException {
          ArrayList<byte[]> missingFamilies = new ArrayList<byte[]>();
          HTableDescriptor td = summaryHTable.getTableDescriptor();
          for ( byte[] columnFamily: columnFamilies ) {
              if ( ! td.hasFamily( columnFamily ) ) {
                  missingFamilies.add( columnFamily );
              }
          }
          
          if ( missingFamilies.size() > 0 ) {
              hadmin.disableTable(MockBI.summaryTableName);
              for ( byte[] columnFamily: missingFamilies ) {
                  HColumnDescriptor hcfd = new HColumnDescriptor( columnFamily );
                  hcfd.setBloomFilterType( BloomType.ROW );
                  hadmin.addColumn(MockBI.summaryTableName, hcfd);
              }
              hadmin.enableTable(MockBI.summaryTableName);
          }
          
      }
      
      public HColumnDescriptor[] getSummaryColumnFamilies() throws IOException {
          return summaryHTable.getTableDescriptor().getColumnFamilies();
      }
      
      public String[] getSummaryColumnFamilyNames() throws IOException {
          HColumnDescriptor[] scfs = getSummaryColumnFamilies();
          String[] scfns = new String[scfs.length];
          for ( int i=0; i < scfns.length; i++ ) {
              scfns[i] = scfs[i].getNameAsString();
          }
          return scfns;      
      }
  }
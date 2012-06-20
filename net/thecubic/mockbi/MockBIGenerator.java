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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import java.util.UUID;

public final class MockBIGenerator {
    
  public static void printPut(Put putin) {
	  System.out.println("Row: " + new String(putin.getRow()) );
	  Map<byte[], List<KeyValue>> familyMap = putin.getFamilyMap();
	  for ( byte[] cf: familyMap.keySet() ) {
		  System.out.println(" CF: " + new String(cf) );
		  for ( KeyValue kv: familyMap.get(cf)) {
			  System.out.print("  KEY(");
			  System.out.print( new String(kv.getQualifier() ) );
			  System.out.print(")=VALUE(");
			  System.out.print( new String(kv.getValue()) );
			  System.out.println(")");
		  }
	  }
  }
  
  public static void main( String[] argv ) throws IOException {
	  MockBIGenerator me = new MockBIGenerator();
	  MockBIHBase mhb = new MockBIHBase();
	  if ( argv.length > 0 ) {
		  int amt = Integer.parseInt(argv[0]);
		  System.out.println("Preparing to insert " + amt + " records");
		  for (int i = 0; i < amt; i++ ) {
			  Put putin = MockBIDatum.genDatum();
			  printPut(putin);
			  mhb.storeLog(putin);
			  System.out.println("Stored.");
		  }
	  } else {
		  ArrayList<Put> batchPut = new ArrayList<Put>();
		  System.out.println("Preparing to insert " + MockBI.nRequests + " records");
		  for (int i = 1; i <= MockBI.nRequests; i++ ) {			
			  batchPut.add( MockBIDatum.genDatum() );
			  if ( i % MockBI.nReport == 0 ) {
				System.out.println("Stored " + i + " records");
				mhb.storeLog(batchPut);
				batchPut.clear();
			  }
		  }
	  }
  }  
}

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
 
//import org.apache.hadoop.hbase.HColumnDescriptor;
import java.io.IOException;

public class MockBIReset {

  public static void main(String[] argv) throws IOException {      
      MockBIHBase hbih = new MockBIHBase();
      hbih.destroyLogTable();
      hbih.createLogTable();
      hbih.destroySummaryTable();
      hbih.createSummaryTable();
      hbih.addSummaryColumn( "SUMMARY_SITE_TENANT_MINUTE".getBytes() );
//      String[] cfns = hbih.getSummaryColumnFamilyNames();
//      for ( String cfn: cfns ) {
//          System.out.println( "CF: " + cfn );
//          MockBISummaryKey mbk = MockBISummaryKey.fromColumnFamilyName( cfn );
//          System.out.println( mbk );
//      }
  }  
}
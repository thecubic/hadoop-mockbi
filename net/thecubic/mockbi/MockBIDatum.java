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
 import org.apache.hadoop.hbase.client.Put;
 import java.util.UUID;
 
 public class MockBIDatum {
    public static final Random rCTX = new Random();    
    public static final byte[] tenant1 = "SpamInc".getBytes();
    public static final byte[] tenant2 = "SpamCo".getBytes();
    public static final byte[] tenant3 = "SpamAG".getBytes();
    public static final byte[] tenant4 = "SpamGmbH".getBytes();    
    public static final byte[] genTenant() {
      float part = rCTX.nextFloat();
      if ( part < 0.3 ) {
        return tenant1;
      } else if ( part < 0.6 ) {
        return tenant2;
      } else if ( part < 0.75 ) {
        return tenant3;
      } else {
        return tenant4;
      }       
    }
    public static final byte[] request1 = "Create".getBytes();
    public static final byte[] request2 = "Retrieve".getBytes();
    public static final byte[] request3 = "Update".getBytes();
    public static final byte[] request4 = "Destroy".getBytes();    
    public static final byte[] genRequest() {
      float part = rCTX.nextFloat();
      if ( part < 0.1 ) {
        return request1;
      } else if ( part < 0.9 ) {
        return request2;
      } else if ( part < 0.92 ) {
        return request3;
      } else {
        return request4;
      }
    }
    public static final byte[] site1 = "RED".getBytes();
    public static final byte[] site2 = "BLUE".getBytes();
    public static final byte[] site3 = "GREEN".getBytes();
    public static final byte[] site4 = "YELLOW".getBytes();
    public static final byte[] genSite() {
      float part = rCTX.nextFloat();
      if ( part < 0.2 ) {
        return site1;
      } else if ( part < 0.4 ) {
        return site2;
      } else if ( part < 0.7 ) {
        return site3;
      } else {
        return site4;
      }
    }    
    public static final long timeMu = 86400000/2;
    public static final long timeSigma = 3600000 * 2;
    public static final byte[] genTimestamp() {      
      long pts = -1;
      do {
          pts = (long) rCTX.nextGaussian() * timeSigma + timeMu;
      } while (pts < 0 || pts > 2*timeMu);      
      return Long.toString( MockBI.baseDate.getTime() + pts ).getBytes();            
    }
    public static final int latencyMu = 75;
    public static final int latencySigma = 20;
    public static final byte[] genLatency() {
      return Integer.toString( (int) Math.abs( rCTX.nextGaussian() * latencySigma + latencyMu ) ).getBytes() ;
    }
    public static final byte[] genPunishedLatency( byte[] drequest, byte[] dsite, byte[] dtenant, byte[] dtimestamp ) {
        int punishedMu = latencyMu;
        int punishedSigma = latencySigma;                        
        
        if ( dsite == site4 ) {
            // yellow is quite mellow
            punishedMu = punishedMu * 4;
        } else if ( dsite == site2 ) {
            // blue has to wait a little bit
            punishedMu = punishedMu * 2;
        } else if ( dsite == site1 ) {
            // red effing loves cocaine
            punishedMu = punishedMu / 2;
        }
        
        if ( drequest == request1 ) {
            // creates are slower
            punishedMu = punishedMu * 2;
        } else if ( drequest == request2 ) {
            // retrieves are really fast
            punishedMu = punishedMu / 2;
        } else if ( drequest == request3 ) {
            // updates are really slower
            punishedMu = punishedMu * 4;
        }
            
        if ( dtenant == tenant1 ) {
           // goofus spaminc never pays for new hardware 
           // and is running on large refrigerator-sized Sun machines           
           punishedMu = punishedMu * 4;
        } else if ( dtenant == tenant2 ) {
           // gallant spamco leverages growth blah blah blah
           punishedMu = punishedMu / 2;
        } else if ( dtenant == tenant3 ) {
           // spamag is just kinda cheap and takes spamco's hand-me-downs
           punishedMu = punishedMu * 2;
        }

      return Integer.toString( (int) Math.abs( rCTX.nextGaussian() * punishedSigma + punishedMu ) ).getBytes() ;  
    }
    public static final String requestName = "request";
    public static final byte[] request = requestName.getBytes();
    public static final String siteName = "site";
    public static final byte[] site = siteName.getBytes();
    public static final String tenantName = "tenant";
    public static final byte[] tenant = tenantName.getBytes();
    public static final String timestampName = "timestamp";
    public static final byte[] timestamp = timestampName.getBytes();
    public static final String latencyName = "latency";
    public static final byte[] latency = latencyName.getBytes();
    
    public static Put genDatum() {
      Put putin = new Put( UUID.randomUUID().toString().getBytes() );      
      byte[] mrequest = genRequest();
      byte[] msite = genSite();
      byte[] mtenant = genTenant();
      byte[] mtimestamp = genTimestamp();
      byte[] mlatency = genPunishedLatency( mrequest, msite, mtenant, mtimestamp );
      putin.add( MockBI.logColumnFamily, request, mrequest );
      putin.add( MockBI.logColumnFamily, site, msite );
      putin.add( MockBI.logColumnFamily, tenant, mtenant );
      putin.add( MockBI.logColumnFamily, timestamp, mtimestamp );    
      putin.add( MockBI.logColumnFamily, latency, mlatency );      
    return putin;
  }
  }
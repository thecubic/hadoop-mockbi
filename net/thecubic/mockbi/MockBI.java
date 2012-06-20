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
 
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Calendar;
import java.util.Collections;

public class MockBI {
    protected static final Map<Integer, String> intervalNameMap;
        static {            
          Map<Integer, String> _ = new HashMap<Integer, String>();
          _.put(Calendar.MINUTE, "MINUTE");
          _.put(Calendar.HOUR, "HOUR");
          _.put(Calendar.DATE, "DAY");
          _.put(Calendar.WEEK_OF_YEAR, "WEEK");      
          intervalNameMap = Collections.unmodifiableMap(_);
        }
        
    protected static final Map<String, Integer> nameIntervalMap;
        static {            
          Map<String, Integer> _ = new HashMap<String, Integer>();
          _.put("MINUTE", Calendar.MINUTE);
          _.put("HOUR", Calendar.HOUR);
          _.put("DAY", Calendar.DATE);
          _.put("WEEK", Calendar.WEEK_OF_YEAR);      
          nameIntervalMap = Collections.unmodifiableMap(_);
        }
        
        
	public static final String logTableName = "MockBILogs";
	public static final byte[] logTable = logTableName.getBytes();
	
	public static final String logColumnFamilyName = "Logs";
    public static final byte[] logColumnFamily = logColumnFamilyName.getBytes();
    
    public static final String summaryTableName = "MockBISummary";
	public static final byte[] summaryTable = summaryTableName.getBytes();	  	
	  
	public static final int nRequests = 1000000;
	public static final int nReport = 1000;
	
	public static Date baseDate;
	static {
			try {
				baseDate = DateFormat.getDateInstance().parse("December 30, 2011");
			} catch (ParseException e) {
				
			}
	}
	  
	public static final byte[] countKey = "count".getBytes();
	public static final byte[] minKey = "min".getBytes();
	public static final byte[] maxKey = "max".getBytes();
	public static final byte[] meanKey = "mean".getBytes();
	public static final byte[] stddevKey = "stddev".getBytes();
}

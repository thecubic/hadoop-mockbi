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
 
 public class RollingNumberSummary {
        public int n;
        public double old_mean, mean, old_stddev, stddev, min, max; 
        public RollingNumberSummary() {
            n = 0;
            old_mean = mean = old_stddev = stddev = 0.0;
            min = Double.POSITIVE_INFINITY;
            max = Double.NEGATIVE_INFINITY;
        }       
        public void push( double x ) {
            n++;
            if ( x > max ) {
                max = x;
            }
            if ( x < min ) {
                min = x;
            }
            if ( n == 1 ) {
                old_mean = mean = x;
            } else {
                mean = old_mean + (x - old_mean) / n;
                stddev = old_stddev + (x - old_mean) * (x - mean);              
                old_mean = mean;
                old_stddev = stddev;                
            }
        }               
        /* sample variance - n should be large */
        public double getVariance() {
            return stddev/(n - 1);
        }
        public double getSD() {
            if ( n > 1 ) {
               return Math.sqrt( getVariance() );
            } else {
               return 0;
            }
        }                       
    }
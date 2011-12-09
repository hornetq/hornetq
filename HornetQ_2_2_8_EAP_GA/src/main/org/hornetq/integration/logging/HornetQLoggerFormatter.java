/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.integration.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.LogRecord;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQLoggerFormatter extends java.util.logging.Formatter
{
   private static String LINE_SEPARATOR = System.getProperty("line.separator");

   private String stripPackage(String clazzName)
   {
      return clazzName.substring(clazzName.lastIndexOf(".") + 1);
   }
   
   private static String [] MONTHS = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

   @Override
   public String format(final LogRecord record)
   {
      Calendar calendar = GregorianCalendar.getInstance();
      calendar.setTimeInMillis(record.getMillis());
      
      StringBuffer sb = new StringBuffer();
        
      sb.append("* [").append(Thread.currentThread().getName()).append("] ");
      sb.append(calendar.get(GregorianCalendar.DAY_OF_MONTH) + "-" + MONTHS[calendar.get(GregorianCalendar.MONTH)] + " " + 
                calendar.get(GregorianCalendar.HOUR_OF_DAY) + ":" +
                calendar.get(GregorianCalendar.MINUTE) +
                ":" +
                calendar.get(GregorianCalendar.SECOND) +
                "," +
                calendar.get(GregorianCalendar.MILLISECOND) +
                " ");
      
      sb.append(record.getLevel()).append(" [");
      sb.append(stripPackage(record.getLoggerName())).append("]").append("  ");
      //sb.append(HornetQLoggerFormatter.LINE_SEPARATOR);
      sb.append(record.getMessage());

      sb.append(HornetQLoggerFormatter.LINE_SEPARATOR);
      if (record.getThrown() != null)
      {
         try
         {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            
            pw.println(record.getThrown() );
            StackTraceElement[] trace = record.getThrown().getStackTrace();
            for (int i=0; i < trace.length; i++)
                pw.println("\tat " + trace[i]);
            pw.close();

            sb.append(sw.toString());
         }
         catch (Exception ex)
         {
         }
      }
      
      sb.append(HornetQLoggerFormatter.LINE_SEPARATOR);

      return sb.toString();
   }

}

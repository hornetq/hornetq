/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.integration.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.LogRecord;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class JBMLoggerFormatter extends java.util.logging.Formatter
{
   Date date = new Date();
   SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss,SSS");
   private String lineSeparator = System.getProperty("line.separator");

   public synchronized String format(LogRecord record)
   {
      StringBuffer sb = new StringBuffer();
      // Minimize memory allocations here.
      date.setTime(record.getMillis());
      sb.append(dateFormat.format(date)).append(" ");
      sb.append(record.getLevel()). append(" [");
      sb.append(record.getLoggerName()).append("]").append("  ");
      sb.append(record.getMessage());
      sb.append(" ");
      sb.append(lineSeparator);
      if (record.getThrown() != null)
      {
         try
         {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            record.getThrown().printStackTrace(pw);
            pw.close();
            sb.append(sw.toString());
         }
         catch (Exception ex)
         {
         }
      }
      return sb.toString();
   }

}

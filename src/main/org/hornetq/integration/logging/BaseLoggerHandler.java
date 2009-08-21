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

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public abstract class BaseLoggerHandler extends Handler
{
   public static final int SEVERE = 1000;

   public static final int WARNING = 900;

   public static final int INFO = 800;

   public static final int CONFIG = 700;

   public static final int FINE = 500;

   public static final int FINER = 400;

   public static final int FINEST = 300;

   public void publish(LogRecord record)
   {
      String loggerName = record.getLoggerName();
      Level level = record.getLevel();
      String message = record.getMessage();
      Throwable throwable = record.getThrown();
      publish(loggerName, level, message, throwable);
   }

   abstract void publish(String loggerName, Level level, String message, Throwable throwable);

   public void flush()
   {
   }

   public void close() throws SecurityException
   {
   }
}

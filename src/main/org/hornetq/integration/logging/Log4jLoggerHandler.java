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

import java.util.logging.Level;

import org.apache.log4j.Logger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class Log4jLoggerHandler extends BaseLoggerHandler
{
   void publish(String loggerName, Level level, String message, Throwable throwable)
   {
      if (throwable == null)
      {
         switch (level.intValue())
         {
            case SEVERE:
            {
               Logger.getLogger(loggerName).error(message);
               break;
            }
            case WARNING:
            {
               Logger.getLogger(loggerName).warn(message);
               break;
            }
            case INFO:
            {
               Logger.getLogger(loggerName).info(message);
               break;
            }
            case CONFIG:
            {
               Logger.getLogger(loggerName).info(message);
               break;
            }
            case FINE:
            {
               Logger.getLogger(loggerName).debug(message);
               break;
            }
            case FINER:
            {
               Logger.getLogger(loggerName).debug(message);
               break;
            }
            case FINEST:
            {
               Logger.getLogger(loggerName).trace(message);
               break;
            }
         }
      }
      else
      {
         switch (level.intValue())
         {
            case SEVERE:
            {
               Logger.getLogger(loggerName).error(message, throwable);
               break;
            }
            case WARNING:
            {
               Logger.getLogger(loggerName).warn(message, throwable);
               break;
            }
            case INFO:
            {
               Logger.getLogger(loggerName).info(message, throwable);
               break;
            }
            case CONFIG:
            {
               Logger.getLogger(loggerName).info(message, throwable);
               break;
            }
            case FINE:
            {
               Logger.getLogger(loggerName).debug(message, throwable);
               break;
            }
            case FINER:
            {
               Logger.getLogger(loggerName).debug(message, throwable);
               break;
            }
            case FINEST:
            {
               Logger.getLogger(loggerName).trace(message, throwable);
               break;
            }
         }
      }
   }

   public void flush()
   {
      //no op
   }

   public void close() throws SecurityException
   {
      //no op
   }
}


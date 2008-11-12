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

import org.apache.log4j.Logger;

import java.util.logging.Level;

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


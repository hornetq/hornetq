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

import org.jboss.logging.LoggerPlugin;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
@SuppressWarnings("deprecation")
public class HornetQLoggerPlugin implements LoggerPlugin
{
   Logger logger;

   public void init(String s)
   {
      logger = Logger.getLogger(s);
   }

   public boolean isTraceEnabled()
   {
      return logger.isLoggable(Level.FINEST);
   }

   public void trace(Object o)
   {
      logger.log(Level.FINEST, o.toString());
   }

   public void trace(Object o, Throwable throwable)
   {
      logger.log(Level.FINEST, o.toString(), throwable);
   }

   public void trace(String s, Object o, Throwable throwable)
   {
      logger.log(Level.FINEST, s + o, throwable);
   }

   public boolean isDebugEnabled()
   {
      return logger.isLoggable(Level.FINE);
   }

   public void debug(Object o)
   {
      logger.log(Level.FINE, o.toString());
   }

   public void debug(Object o, Throwable throwable)
   {
      logger.log(Level.FINE, o.toString(), throwable);
   }

   public void debug(String s, Object o, Throwable throwable)
   {
      logger.log(Level.FINE, s + o, throwable);
   }

   public boolean isInfoEnabled()
   {
      return logger.isLoggable(Level.INFO);
   }

   public void info(Object o)
   {
      logger.log(Level.INFO, o.toString());
   }

   public void info(Object o, Throwable throwable)
   {
      logger.log(Level.INFO, o.toString(), throwable);
   }

   public void info(String s, Object o, Throwable throwable)
   {
      logger.log(Level.INFO, s + o, throwable);
   }

   public void warn(Object o)
   {
      logger.log(Level.WARNING, o.toString());
   }

   public void warn(Object o, Throwable throwable)
   {
      logger.log(Level.WARNING, o.toString(), throwable);
   }

   public void warn(String s, Object o, Throwable throwable)
   {
      logger.log(Level.WARNING, s + o, throwable);
   }

   public void error(Object o)
   {
      logger.log(Level.SEVERE, o.toString());
   }

   public void error(Object o, Throwable throwable)
   {
      logger.log(Level.SEVERE, o.toString(), throwable);
   }

   public void error(String s, Object o, Throwable throwable)
   {
      logger.log(Level.SEVERE, s + o, throwable);
   }

   public void fatal(Object o)
   {
      logger.log(Level.SEVERE, o.toString());
   }

   public void fatal(Object o, Throwable throwable)
   {
      logger.log(Level.SEVERE, o.toString(), throwable);
   }

   public void fatal(String s, Object o, Throwable throwable)
   {
      logger.log(Level.SEVERE, s + o, throwable);
   }
}

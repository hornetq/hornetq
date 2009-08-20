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

package org.hornetq.core.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * 
 * A Logger
 * 
 * For now just delegates to org.jboss.util.Logger
 * 
 * This class allows us to isolate all our logging dependencies in one place
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Logger
{
   private static final ConcurrentMap<Class<?>, Logger> loggers = new ConcurrentHashMap<Class<?>, Logger>();
   
   public static Logger getLogger(final Class<?> clazz)
   {
      Logger logger = loggers.get(clazz);
      
      if (logger == null)
      {
         logger = new Logger(clazz);
         
         Logger oldLogger = loggers.putIfAbsent(clazz, logger);
         
         if (oldLogger != null)
         {
            logger = oldLogger;
         }
      }      
      
      return logger;
   }
   
   private final java.util.logging.Logger logger;
   
   private Logger(final Class<?> clazz)
   {
      logger = java.util.logging.Logger.getLogger(clazz.getName());
      //logger.setUseParentHandlers(false);
   }
   
   public boolean isInfoEnabled()
   {
      return logger.isLoggable(Level.INFO);
   }
   
   public boolean isDebugEnabled()
   {
      return logger.isLoggable(Level.FINE);
   }
   
   public boolean isTraceEnabled()
   {
      return logger.isLoggable(Level.FINEST);
   }
   
   public void fatal(final Object message)
   {
     logger.log(Level.SEVERE, message==null?"NULL":message.toString());
   }
   
   public void fatal(final Object message, final Throwable t)
   {
      logger.log(Level.SEVERE, message==null?"NULL":message.toString(), t);
   }
   
   public void error(final Object message)
   {
      logger.log(Level.SEVERE, message==null?"NULL":message.toString());
   }
   
   public void error(final Object message, final Throwable t)
   {
      logger.log(Level.SEVERE, message==null?"NULL":message.toString(), t);
   }
   
   public void warn(final Object message)
   {
      logger.log(Level.WARNING, message==null?"NULL":message.toString());
   }
   
   public void warn(final Object message, final Throwable t)
   {
      logger.log(Level.WARNING, message==null?"NULL":message.toString(), t);
   }
   
   public void info(final Object message)
   {
      logger.log(Level.INFO, message==null?"NULL":message.toString());
   }
   
   public void info(final Object message, final Throwable t)
   {
      logger.log(Level.INFO, message==null?"NULL":message.toString(), t);
   }
   
   public void debug(final Object message)
   {
      logger.log(Level.FINE, message==null?"NULL":message.toString());
   }
   
   public void debug(final Object message, final Throwable t)
   {
      logger.log(Level.FINE, message==null?"NULL":message.toString(), t);
   }
   
   public void trace(final Object message)
   {
      logger.log(Level.FINEST, message==null?"NULL":message.toString());
   }
   
   public void trace(final Object message, final Throwable t)
   {
      logger.log(Level.FINEST, message==null?"NULL":message.toString(), t);
   }
   
}

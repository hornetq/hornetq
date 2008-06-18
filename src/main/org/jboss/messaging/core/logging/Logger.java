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

package org.jboss.messaging.core.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
   
   private final org.jboss.logging.Logger logger;
   
   private Logger(final Class<?> clazz)
   {
      if(!"org.jboss.logging.Log4jLoggerPlugin.class".equals(org.jboss.logging.Logger.getPluginClassName()))
      {
         org.jboss.logging.Logger.setPluginClassName("org.jboss.messaging.core.logging.JBMLoggerPlugin");
      }
      logger = org.jboss.logging.Logger.getLogger(clazz);
   }
   
   public boolean isInfoEnabled()
   {
      return logger.isInfoEnabled();
   }
   
   public boolean isDebugEnabled()
   {
      return logger.isDebugEnabled();
   }
   
   public boolean isTraceEnabled()
   {
      return logger.isTraceEnabled();
   }
   
   public void fatal(final Object message)
   {
      logger.fatal(message);
   }
   
   public void fatal(final Object message, final Throwable t)
   {
      logger.fatal(message, t);
   }
   
   public void error(final Object message)
   {
      logger.error(message);
   }
   
   public void error(final Object message, final Throwable t)
   {
      logger.error(message, t);
   }
   
   public void warn(final Object message)
   {
      logger.warn(message);
   }
   
   public void warn(final Object message, final Throwable t)
   {
      logger.warn(message, t);
   }
   
   public void info(final Object message)
   {
      logger.info(message);
   }
   
   public void info(final Object message, final Throwable t)
   {
      logger.info(message, t);
   }
   
   public void debug(final Object message)
   {
      logger.debug(message);
   }
   
   public void debug(final Object message, final Throwable t)
   {
      logger.debug(message, t);
   }
   
   public void trace(final Object message)
   {
      logger.trace(message);
   }
   
   public void trace(final Object message, final Throwable t)
   {
      logger.trace(message, t);
   }
   
}

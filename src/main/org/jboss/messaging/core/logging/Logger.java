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
import java.util.logging.Level;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

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

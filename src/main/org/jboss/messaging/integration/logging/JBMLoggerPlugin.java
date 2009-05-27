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

import org.jboss.logging.LoggerPlugin;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
@SuppressWarnings("deprecation")
public class JBMLoggerPlugin implements LoggerPlugin
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

   public void warn(Object o)
   {
      logger.log(Level.WARNING, o.toString());
   }

   public void warn(Object o, Throwable throwable)
   {
      logger.log(Level.WARNING, o.toString(), throwable);
   }

   public void error(Object o)
   {
      logger.log(Level.SEVERE, o.toString());
   }

   public void error(Object o, Throwable throwable)
   {
      logger.log(Level.SEVERE, o.toString(), throwable);
   }

   public void fatal(Object o)
   {
      logger.log(Level.SEVERE, o.toString());
   }

   public void fatal(Object o, Throwable throwable)
   {
      logger.log(Level.SEVERE, o.toString(), throwable);
   }
}

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

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JBMLoggerPlugin implements LoggerPlugin
{
   org.apache.log4j.Logger logger;

   public void init(String s)
   {
      logger = org.apache.log4j.Logger.getLogger(s);
   }

   public boolean isTraceEnabled()
   {
      return logger.isTraceEnabled();
   }

   public void trace(Object o)
   {
      logger.trace(o);
   }

   public void trace(Object o, Throwable throwable)
   {
      logger.trace(o, throwable);
   }

   public boolean isDebugEnabled()
   {
      return logger.isDebugEnabled();
   }

   public void debug(Object o)
   {
      logger.debug(o);
   }

   public void debug(Object o, Throwable throwable)
   {
      logger.debug(o, throwable);
   }

   public boolean isInfoEnabled()
   {
      return logger.isInfoEnabled();
   }

   public void info(Object o)
   {
      logger.info(o);
   }

   public void info(Object o, Throwable throwable)
   {
      logger.info(o, throwable);
   }

   public void warn(Object o)
   {
      logger.warn(o);
   }

   public void warn(Object o, Throwable throwable)
   {
      logger.warn(o, throwable);
   }

   public void error(Object o)
   {
      logger.warn(o);
   }

   public void error(Object o, Throwable throwable)
   {
      logger.error(o, throwable);
   }

   public void fatal(Object o)
   {
      logger.fatal(o);
   }

   public void fatal(Object o, Throwable throwable)
   {
      logger.fatal(o, throwable);
   }
}

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

import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Level;

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

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.plugin;

import javax.transaction.TransactionManager;

import org.jboss.jms.util.ExceptionUtil;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;
import org.jboss.messaging.core.plugin.contract.ShutdownLogger;

/**
 * A JDBCShutdownLoggerService
 * 
 * MBean wrapper around a ShutDownLogger
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class JDBCShutdownLoggerService extends JDBCServiceSupport
{
   private ShutdownLogger shutdownLogger;
   
   private boolean started;
   
   // Constructors ---------------------------------------------------------
   
   public JDBCShutdownLoggerService()
   {      
   }
   
   // ServerPlugin implementation ------------------------------------------
   
   public MessagingComponent getInstance()
   {
      return shutdownLogger;
   }
   
   // ServiceMBeanSupport overrides ---------------------------------
   
   protected synchronized void startService() throws Exception
   {
      if (started)
      {
         throw new IllegalStateException("Service is already started");
      }
      
      super.startService();
      
      try
      {  
         TransactionManager tm = getTransactionManagerReference();
                  
         shutdownLogger = new JDBCShutdownLogger(ds, tm, sqlProperties, createTablesOnStartup);
         
         shutdownLogger.start();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      } 
   }
   
   protected void stopService() throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Service is not started");
      }
      
      shutdownLogger.stop();
      shutdownLogger = null;
      
      started = false;
      
      log.debug(this + " stopped");
   }
}

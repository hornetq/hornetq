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
package org.jboss.jms.server.plugin;

import javax.transaction.TransactionManager;

import org.jboss.jms.server.plugin.contract.JMSUserManager;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.messaging.core.plugin.JDBCServiceSupport;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;

/**
 * A JDBCJMSUserManagerService
 * 
 * MBean wrapper for a JMSUserManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class JDBCJMSUserManagerService extends JDBCServiceSupport
{
   private JMSUserManager userManager;
   
   private boolean started;
   
   // Constructors -----------------------------------------------------
   
   public JDBCJMSUserManagerService()
   {      
   }
   
   // ServerPlugin implementation ------------------------------------------
   
   public MessagingComponent getInstance()
   {
      return userManager;
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
         
         userManager = new JDBCJMSUserManager(ds, tm, sqlProperties, createTablesOnStartup);
         
         userManager.start();
         
         started = true;         
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
      
      super.stopService();
      
      try
      {      
         userManager.stop();
         
         started = false;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      } 
   }
}

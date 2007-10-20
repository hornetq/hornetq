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
package org.jboss.messaging.core.jmx;

import javax.transaction.TransactionManager;

import org.jboss.messaging.core.contract.MessagingComponent;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.impl.JDBCPersistenceManager;
import org.jboss.messaging.util.ExceptionUtil;

/**
 * A JDBCPersistenceManagerService
 * 
 * MBean wrapper around a JDBCPersistenceManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2684 $</tt>
 *
 * $Id: JDBCPersistenceManagerService.java 2684 2007-05-15 07:31:30Z timfox $
 *
 */
public class JDBCPersistenceManagerService extends JDBCServiceSupport
{
   private PersistenceManager persistenceManager;
   
   private boolean started;
   
   private boolean usingBatchUpdates;
   
   private boolean usingBinaryStream = true;
   
   private boolean usingTrailingByte;
   
   private int maxParams = 100;
   
   private long reaperPeriod = 5000;
   
   private boolean supportsBlobOnSelect = true;
   
   // Constructors --------------------------------------------------------
   
   public JDBCPersistenceManagerService()
   {      
   }
   
   // ServerPlugin implementation ------------------------------------------
   
   public MessagingComponent getInstance()
   {
      return persistenceManager;
   }
   
   // ServiceMBeanSupport overrides -----------------------------------------
   
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
         
         persistenceManager =
            new JDBCPersistenceManager(ds, tm, sqlProperties,
                                       createTablesOnStartup, usingBatchUpdates,
                                       usingBinaryStream, usingTrailingByte, maxParams, reaperPeriod,
                                       supportsBlobOnSelect);
         
         persistenceManager.start();
         
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
      
      try
      {
         persistenceManager.stop();
         
         persistenceManager = null;
         
         started = false;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      } 
      
      log.debug(this + " stopped");
   }
   
   // MBean attributes -------------------------------------------------------
   
   public boolean isUsingBatchUpdates()
   {
      return usingBatchUpdates;
   }
   
   public void setUsingBatchUpdates(boolean b)
   {
      usingBatchUpdates = b;
   }
   
   public int getMaxParams()
   {
      return maxParams;
   }
   
   public void setMaxParams(int maxParams)
   {
      this.maxParams = maxParams;
   }
   
   public boolean isUsingBinaryStream()
   {
      return usingBinaryStream;
   }
   
   public void setUsingBinaryStream(boolean b)
   {
      usingBinaryStream = b;
   }
   
   public boolean isUsingTrailingByte()
   {
      return usingTrailingByte;
   }
   
   public void setUsingTrailingByte(boolean b)
   {
      usingTrailingByte = b;
   }
   
   public void setReaperPeriod(long reaperPeriod)
   {
   	if (reaperPeriod < 0)
   	{
   		throw new IllegalArgumentException("reaperPeriod must be >= 0");
   	}
   	
   	this.reaperPeriod = reaperPeriod;
   }
   
   public long getReaperPeriod()
   {
   	return reaperPeriod;
   }
   
   public boolean isSupportsBlobOnSelect()
   {
   	return supportsBlobOnSelect;
   }
   
   public void setSupportsBlobOnSelect(boolean b)
   {
   	log.info("Calling set blob on select " + b);
   	this.supportsBlobOnSelect = b;
   }
      
}

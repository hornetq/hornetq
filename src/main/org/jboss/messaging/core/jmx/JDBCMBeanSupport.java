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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.system.ServiceMBeanSupport;
import org.jboss.tm.TransactionManagerLocator;

/**
 * A JDBCMBeanSupport
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class JDBCMBeanSupport extends ServiceMBeanSupport
{
   protected DataSource ds;
   
   protected Properties sqlProperties;
         
   protected TransactionManager tm;
      
   private String dataSourceJNDIName;
   
   private boolean createTablesOnStartup = true;
   
   private ObjectName tmObjectName;
     
   public JDBCMBeanSupport()
   {
      sqlProperties = new Properties();
   }   
   
   // ServiceMBeanSupport overrides ---------------------------------
   
   protected void startService() throws Exception
   {
      try
      {
         if (ds == null)
         {
            InitialContext ic = new InitialContext();
            ds = (DataSource)ic.lookup(dataSourceJNDIName);
            ic.close();
         }
         
         if (ds == null)
         {
            throw new IllegalStateException("No DataSource found. This service dependencies must " +
            "have not been enforced correctly!");
         }
         
         log.debug(this + " started");
         
         tm = getTransactionManagerReference();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      } 
   }
   
   protected void stopService() throws Exception
   {
      log.debug(this + " stopped");
   }
  
   // MBean attributes --------------------------------------------------------
      
   public String getSqlProperties()
   {
      try
      {
         ByteArrayOutputStream boa = new ByteArrayOutputStream();
         sqlProperties.store(boa, "");
         return new String(boa.toByteArray());
      }
      catch (IOException shouldnothappen)
      {
         return "";
      }
   }
   
   public void setSqlProperties(String value)
   {
      try
      {         
         ByteArrayInputStream is = new ByteArrayInputStream(value.getBytes());
         sqlProperties = new Properties();
         sqlProperties.load(is);         
      }
      catch (IOException shouldnothappen)
      {
         log.error("Caught IOException", shouldnothappen);
      }
   }
      
   public void setDataSource(String dataSourceJNDIName) throws Exception
   {
      this.dataSourceJNDIName = dataSourceJNDIName;
   }
   
   public String getDataSource()
   {
      return dataSourceJNDIName;
   }
   
   public void setTransactionManager(ObjectName tmObjectName) throws Exception
   {
      this.tmObjectName = tmObjectName;
   }
   
   public ObjectName getTransactionManager()
   {
      return tmObjectName;
   }

   public boolean isCreateTablesOnStartup() throws Exception
   {
      return createTablesOnStartup;
   }

   public void setCreateTablesOnStartup(boolean b) throws Exception
   {
      createTablesOnStartup = b;
   }
   
   // Protected ----------------------------------------------------------     
   
   // Private ----------------------------------------------------------------
   
   private TransactionManager getTransactionManagerReference()
   {
      // lazy initialization
      if (tm == null)
      {
         tm = TransactionManagerLocator.getInstance().locate();
      }

      return tm;
   }
           
   // Innner classes ---------------------------------------------------------   
}

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.jboss.jms.util.ExceptionUtil;
import org.jboss.messaging.core.plugin.contract.ServerPlugin;
import org.jboss.system.ServiceMBeanSupport;
import org.jboss.tm.TransactionManagerServiceMBean;

/**
 * A JDBCServiceSupport
 *
 * Common functionality for any service that needs to access a database
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class JDBCServiceSupport extends ServiceMBeanSupport implements ServerPlugin
{   
   protected DataSource ds;
   
   protected Properties sqlProperties;
         
   // never access directly
   private TransactionManager tm;
    
   private Map defaultDMLStatements;
   
   private Map defaultDDLStatements;
   
   private String dataSourceJNDIName;
   
   private boolean createTablesOnStartup = true;
   
   private ObjectName tmObjectName;
   
   
   public JDBCServiceSupport()
   {
      defaultDMLStatements = new HashMap();
      
      defaultDDLStatements = new HashMap();
      
      sqlProperties = new Properties();
   }
   
   /*
    * This constructor should only be used for testing
    */
   public JDBCServiceSupport(DataSource ds, TransactionManager tm)
   {
      this();
      
      log.info(this + " being created, tx mgr is " + tm);
      
      this.ds = ds;
      
      this.tm = tm;
   }
   
   
   // ServerPlugin implementation ------------------------------------------
   
   public Object getInstance()
   {
      return this;
   }
   
   // ServiceMBeanSupport overrides ---------------------------------
   
   protected void startService() throws Exception
   {
      log.info(this + " startService");
      
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
         
         defaultDMLStatements.putAll(getDefaultDMLStatements());
         
         defaultDDLStatements.putAll(getDefaultDDLStatements());
                  
         //Validate the SQL properties
         Iterator iter = this.sqlProperties.keySet().iterator();
         
         while (iter.hasNext())
         {
            String statementName = (String)iter.next();
            
            //This will throw an exception if there is no default for the statement specified in the
            //sql properties
            getSQLStatement(statementName);
         }
         
         //Also we validate the other way - if there are any sql properties specified then there should be one for every
         //default property
         //This allows us to catch subtle errors in the persistence manager configuration
         if (!sqlProperties.isEmpty())
         {
            iter = defaultDMLStatements.keySet().iterator();
            
            while (iter.hasNext())
            {
               String statementName = (String)iter.next();
               
               if (sqlProperties.get(statementName) == null)
               {
                  throw new IllegalStateException("SQL statement " + statementName + " is not specified in the persistence manager SQL properties");
               }
            }
            
            iter = defaultDDLStatements.keySet().iterator();
            
            while (iter.hasNext())
            {
               String statementName = (String)iter.next();
               
               if (sqlProperties.get(statementName) == null)
               {
                  throw new IllegalStateException("SQL statement " + statementName + " is not specified in the persistence manager SQL properties");
               }
            }
         }
         
         if (createTablesOnStartup)
         {
            createSchema();
         }
         
         log.debug(this + " started");
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
   
   protected String getSQLStatement(String statementName)
   {
      String defaultStatement = (String)defaultDMLStatements.get(statementName);
      
      if (defaultStatement == null)
      {
         defaultStatement = (String)defaultDDLStatements.get(statementName);
      }
      
      if (defaultStatement == null)
      {
         throw new IllegalArgumentException("No such SQL statement: " + statementName);
      }
      
      String statement = sqlProperties.getProperty(statementName, defaultStatement);
      
      return statement;
   }
   
   protected abstract Map getDefaultDDLStatements();
   
   protected abstract Map getDefaultDMLStatements();
   
   
   // Private ----------------------------------------------------------------
   
   private TransactionManager getTransactionManagerReference()
   {
      // lazy initialization
      if (tm == null)
      {
         TransactionManagerServiceMBean tms =
            (TransactionManagerServiceMBean)MBeanServerInvocationHandler.
            newProxyInstance(getServer(), tmObjectName, TransactionManagerServiceMBean.class, false);

         tm = tms.getTransactionManager();
      }

      return tm;
   }
           
   private void createSchema() throws Exception
   {      
      Connection conn = null;      
      TransactionWrapper tx = new TransactionWrapper();
      
      log.info(this + " createSchema");
      
      try
      {
         conn = ds.getConnection();
         
         Iterator iter = defaultDDLStatements.keySet().iterator();
         
         while (iter.hasNext())
         {
            String statementName = (String)iter.next();
             
            String statement = getSQLStatement(statementName);
            
            try
            {
               if (log.isTraceEnabled()) { log.trace("Executing: " + statement); }
                   
               conn.createStatement().executeUpdate(statement);
            }
            catch (SQLException e) 
            {
               log.debug("Failed to execute: " + statement, e);
            }  
         }      
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable t)
            {}
         }
         tx.end();
      }      
   }      
   
   // Innner classes ---------------------------------------------------------
   
   protected class TransactionWrapper
   {
      private javax.transaction.Transaction oldTx;
      
      private boolean failed;
      
      public TransactionWrapper() throws Exception
      {
         TransactionManager tm = getTransactionManagerReference();

         oldTx = tm.suspend();
         
         tm.begin();
      }
      
      public void end() throws Exception
      {
         TransactionManager tm = getTransactionManagerReference();

         try
         {
            if (Status.STATUS_MARKED_ROLLBACK == tm.getStatus())
            {
               failed = true;
               tm.rollback();               
            }
            else
            {
               tm.commit();
            }
         }
         finally
         {
            if (oldTx != null && tm != null)
            {
               tm.resume(oldTx);
            }
         }
      }      
      
      public void exceptionOccurred() throws Exception
      {
         getTransactionManagerReference().setRollbackOnly();
      }
      
      public boolean isFailed()
      {
         return failed;
      }
   }
}

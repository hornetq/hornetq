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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;

/**
 * 
 * A JDBCSupport
 * 
 * Common functionality for messaging components that need to access a database
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class JDBCSupport implements MessagingComponent
{
   private static final Logger log = Logger.getLogger(JDBCSupport.class);
      
   protected DataSource ds;
   
   private TransactionManager tm;
      
   protected Properties sqlProperties;
         
   private Map defaultDMLStatements;
   
   private Map defaultDDLStatements;
       
   private boolean createTablesOnStartup = true;
      
   public JDBCSupport()
   {
      defaultDMLStatements = new HashMap();
      
      defaultDDLStatements = new HashMap();
      
      sqlProperties = new Properties();
   }
   
   public JDBCSupport(DataSource ds, TransactionManager tm, Properties sqlProperties,
                      boolean createTablesOnStartup)
   {
      this();
      
      this.ds = ds;
      
      this.tm = tm;
      
      if (sqlProperties != null)
      {
         this.sqlProperties = sqlProperties;
      }
      
      this.createTablesOnStartup = createTablesOnStartup;
   }
      
   // MessagingComponent overrides ---------------------------------
   
   public void start() throws Exception
   {
      log.info("In jdbcsupport::start");
      
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
   
   public void stop() throws Exception
   {
      log.debug(this + " stopped");
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
   
   protected Map getDefaultDMLStatements()
   {                
      return Collections.EMPTY_MAP;
   }
   
   protected Map getDefaultDDLStatements()
   {
      return Collections.EMPTY_MAP;
   }
   
   // Private ----------------------------------------------------------------
              
   private void createSchema() throws Exception
   {      
      Connection conn = null;      
      TransactionWrapper tx = new TransactionWrapper();
      
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
         oldTx = tm.suspend();
         
         tm.begin();
      }
      
      public void end() throws Exception
      {
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
         tm.setRollbackOnly();
      }
      
      public boolean isFailed()
      {
         return failed;
      }
   }
}

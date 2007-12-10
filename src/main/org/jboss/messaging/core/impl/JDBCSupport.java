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
package org.jboss.messaging.core.impl;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.MessagingComponent;

import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Common functionality for messaging components that need to access a database.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 2726 $</tt>
 *
 * $Id: JDBCSupport.java 2726 2007-05-24 19:25:06Z timfox $
 *
 */
public class JDBCSupport implements MessagingComponent
{
   private static final Logger log = Logger.getLogger(JDBCSupport.class);

   private static boolean trace = log.isTraceEnabled();

   protected DataSource ds;

   private TransactionManager tm;

   protected Properties sqlProperties;

   private Map defaultDMLStatements;

   private Map defaultDDLStatements;

   private boolean createTablesOnStartup = true;

   public JDBCSupport()
   {
      defaultDMLStatements = new LinkedHashMap();

      defaultDDLStatements = new LinkedHashMap();

      sqlProperties = new Properties();
   }

   // MessagingComponent overrides ---------------------------------

   public void start() throws Exception
   {
      defaultDMLStatements.putAll(getDefaultDMLStatements());

      defaultDDLStatements.putAll(getDefaultDDLStatements());

      // Make a copy without startup statements
      Properties sqlPropertiesCopy = new Properties();

      for (Iterator iterSQL = sqlProperties.entrySet().iterator(); iterSQL.hasNext();)
      {
         Map.Entry entry = (Map.Entry) iterSQL.next();
         if (!this.ignoreVerificationOnStartup((String)entry.getKey()))
         {
            sqlPropertiesCopy.put(entry.getKey(), entry.getValue());
         }
      }

      //Validate the SQL properties
      Iterator iter = sqlPropertiesCopy.keySet().iterator();

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
      if (!sqlPropertiesCopy.isEmpty())
      {
         iter = defaultDMLStatements.keySet().iterator();

         while (iter.hasNext())
         {
            String statementName = (String)iter.next();

            if (sqlProperties.get(statementName) == null)
            {
               throw new IllegalStateException("SQL statement " + statementName + " is not specified in the SQL properties");
            }
         }

         iter = defaultDDLStatements.keySet().iterator();

         while (iter.hasNext())
         {
            String statementName = (String)iter.next();

            if (sqlPropertiesCopy.get(statementName) == null)
            {
               throw new IllegalStateException("SQL statement " + statementName + " is not specified in the SQL properties");
            }
         }
      }

      if (createTablesOnStartup)
      {
         createSchema();
      }
      else
      {
        log.debug("Schema is not being created as createTablesOnStartup=" + createTablesOnStartup);
      }
   }

   public void stop() throws Exception
   {
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

      return sqlProperties.getProperty(statementName, defaultStatement);
   }

   protected Map getDefaultDMLStatements()
   {
      return Collections.EMPTY_MAP;
   }

   protected Map getDefaultDDLStatements()
   {
      return Collections.EMPTY_MAP;
   }

   /** Subclasses might choose to not cross check the maps on certain statements.
    *  An example would be POPULATE.TABLES on JDBCJMSUserManagerService */
   protected boolean ignoreVerificationOnStartup(String statementName)
   {
      return false;
   }

   protected void closeResultSet(ResultSet rs)
   {
   	if (rs != null)
      {
         try
         {
            rs.close();
         }
         catch (Throwable e)
         {
         	if (trace)
         	{
         		log.trace("Failed to close result set", e);
         	}
         }
      }
   }

   protected void closeStatement(Statement st)
   {
   	if (st != null)
      {
         try
         {
         	st.close();
         }
         catch (Throwable e)
         {
         	if (trace)
         	{
         		log.trace("Failed to close statement", e);
         	}
         }
      }
   }

   protected void closeConnection(Connection conn)
   {
   	if (conn != null)
      {
         try
         {
         	conn.close();
         }
         catch (Throwable e)
         {
         	if (trace)
         	{
         		log.trace("Failed to close statement", e);
         	}
         }
      }
   }
   // Private ----------------------------------------------------------------

   private void createSchema() throws Exception
   {
      // Postgresql will not process any further commands in a transaction after a create table
      // fails: org.postgresql.util.PSQLException: ERROR: current transaction is aborted, commands
      // ignored until end of transaction block. Therefore we need to ensure each CREATE is executed
      // in its own transaction

      for (Iterator i = defaultDDLStatements.keySet().iterator(); i.hasNext(); )
      {
         Connection conn = null;

         Statement st = null;

         TransactionWrapper tx = new TransactionWrapper();

         try
         {
            conn = ds.getConnection();

            String statementName = (String)i.next();

            String statement = getSQLStatement(statementName);

            if (!"IGNORE".equals(statement))
            {
               try
               {
                  if (log.isTraceEnabled()) { log.trace("Executing: " + statement); }

                  st = conn.createStatement();

                  st.executeUpdate(statement);
               }
               catch (Exception e)
               {
                  log.debug("Failed to execute: " + statement, e);

                  tx.exceptionOccurred();
               }
            }
            else
            {
                log.debug("createSchema ignoring statement for " + statementName);
            }
         }
         finally
         {
         	closeStatement(st);

            closeConnection(conn);

            tx.end();
         }
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
               if (trace) { log.trace("Rolling back tx"); }
               tm.rollback();
            }
            else
            {
               if (trace) { log.trace("Committing tx"); }
               tm.commit();
            }
         }
         finally
         {
            if (oldTx != null && tm != null)
            {
               if (trace) { log.trace("Resuming tx"); }
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

   protected abstract class JDBCTxRunner<T>
   {
   	private static final int MAX_TRIES = 25;

   	protected Connection conn;

      private TransactionWrapper wrap;

      public T execute() throws Exception
		{
	      wrap = new TransactionWrapper();

	      try
	      {
	         conn = ds.getConnection();

	         return doTransaction();
	      }
	      catch (Exception e)
	      {
	         wrap.exceptionOccurred();
	         throw e;
	      }
	      finally
	      {
            wrap.end();
	      	closeConnection(conn);
	      }
		}

		public T executeWithRetry() throws Exception
		{
	      int tries = 0;

	      while (true)
	      {
	         try
	         {
	            T res = execute();

	            if (tries > 0)
	            {
	               log.warn("Update worked after retry");
	            }
	            return res;
	         }
	         catch (SQLException  e)
	         {
  	            log.warn("SQLException caught, SQLState " + e.getSQLState() + " code:" + e.getErrorCode() + "- assuming deadlock detected, try:" + (tries + 1), e);

	            tries++;
	            if (tries == MAX_TRIES)
	            {
	               log.error("Retried " + tries + " times, now giving up");
	               throw new IllegalStateException("Failed to excecute transaction");
	            }
	            log.warn("Trying again after a pause");
	            //Now we wait for a random amount of time to minimise risk of deadlock
	            Thread.sleep((long)(Math.random() * 500));
	         }
	      }
		}

		public abstract T doTransaction() throws Exception;
   }


   protected abstract class JDBCTxRunner2<T>
   {
      private static final int MAX_TRIES = 25;

      protected Connection conn;

      public T execute() throws Exception
      {
         Transaction tx = tm.suspend();

         try
         {
            conn = ds.getConnection();

            conn.setAutoCommit(false);

            T res = doTransaction();

            conn.commit();

            return res;
         }
         catch (Exception e)
         {
            try
            {
               conn.rollback();
            }
            catch (Throwable t)
            {
               log.trace("Failed to rollback", t);
            }

            throw e;
         }
         finally
         {
            closeConnection(conn);

            if (tx != null)
            {
               tm.resume(tx);
            }
         }
      }

      public T executeWithRetry() throws Exception
      {
         int tries = 0;

         while (true)
         {
            try
            {
               T res = execute();

               if (tries > 0)
               {
                  log.warn("Update worked after retry");
               }
               return res;
            }
            catch (SQLException  e)
            {
               log.warn("SQLException caught, SQLState " + e.getSQLState() + " code:" + e.getErrorCode() + "- assuming deadlock detected, try:" + (tries + 1), e);

               tries++;
               if (tries == MAX_TRIES)
               {
                  log.error("Retried " + tries + " times, now giving up");
                  throw new IllegalStateException("Failed to excecute transaction", e);
               }
               log.warn("Trying again after a pause");
               //Now we wait for a random amount of time to minimise risk of deadlock
               Thread.sleep((long)(Math.random() * 500));
            }
         }
      }

      public abstract T doTransaction() throws Exception;
   }

   public TransactionManager getTm()
    {
        return tm;
    }

    public void setTm(TransactionManager tm)
    {
        this.tm = tm;
    }

    public Properties getSqlProperties()
    {
        return sqlProperties;
    }

    public void setSqlProperties(Properties sqlProperties)
    {
        this.sqlProperties = sqlProperties;
    }

    public boolean isCreateTablesOnStartup()
    {
        return createTablesOnStartup;
    }

    public void setCreateTablesOnStartup(boolean createTablesOnStartup)
    {
        this.createTablesOnStartup = createTablesOnStartup;
    }


    public DataSource getDs()
    {
        return ds;
    }

    public void setDs(DataSource ds)
    {
        this.ds = ds;
    }
}

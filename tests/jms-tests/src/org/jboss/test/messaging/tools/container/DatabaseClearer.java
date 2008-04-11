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
package org.jboss.test.messaging.tools.container;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.messaging.core.logging.Logger;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class DatabaseClearer
{
   private static final Logger log = Logger.getLogger(DatabaseClearer.class);
   private DataSource dataSource;
   private TransactionManager transactionManager;

   public DataSource getDataSource()
   {
      return dataSource;
   }

   public void setDataSource(DataSource dataSource)
   {
      this.dataSource = dataSource;
   }

   public TransactionManager getTransactionManager()
   {
      return transactionManager;
   }

   public void setTransactionManager(TransactionManager transactionManager)
   {
      this.transactionManager = transactionManager;
   }


   public void deleteAllData() throws Exception
   {
      log.debug("DELETING ALL DATA FROM DATABASE!");


      // We need to execute each drop in its own transaction otherwise postgresql will not execute
      // further commands after one fails


      javax.transaction.Transaction txOld = transactionManager.suspend();

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_POSTOFFICE");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_MSG_REF");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_MSG");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_TX");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_COUNTER");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_USER");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_ROLE");

      if (txOld != null)
      {
         transactionManager.resume(txOld);
      }

      log.debug("done with the deleting data");
   }

   public void deleteData() throws Exception
   {
      log.debug("DELETING ALL DATA FROM DATABASE!");


      // We need to execute each drop in its own transaction otherwise postgresql will not execute
      // further commands after one fails


      javax.transaction.Transaction txOld = transactionManager.suspend();

      //executeStatement(transactionManager, dataSource, "DELETE FROM JBM_POSTOFFICE");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_MSG_REF");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_MSG");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_TX");

      //executeStatement(transactionManager, dataSource, "DELETE FROM JBM_COUNTER");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_USER");

      executeStatement(transactionManager, dataSource, "DELETE FROM JBM_ROLE");

      if (txOld != null)
      {
         transactionManager.resume(txOld);
      }

      log.debug("done with the deleting data");
   }

   private void executeStatement(TransactionManager mgr, DataSource ds, String statement) throws Exception
   {
      Connection conn = null;
      boolean exception = false;

      try
      {
         try
         {
            mgr.begin();

            conn = ds.getConnection();

            log.debug("executing " + statement);

            PreparedStatement ps = conn.prepareStatement(statement);

            ps.executeUpdate();

            log.debug(statement + " executed");

            ps.close();
         }
         catch (SQLException e)
         {
            // Ignore
            log.debug("Failed to execute statement", e);
            exception = true;
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         if (exception)
         {
            mgr.rollback();
         }
         else
         {
            mgr.commit();
         }
      }


   }
}

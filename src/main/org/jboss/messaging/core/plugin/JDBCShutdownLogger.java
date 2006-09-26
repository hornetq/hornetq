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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.ShutdownLogger;

/**
 * 
 * A JDBCShutdownLogger
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class JDBCShutdownLogger extends JDBCSupport implements ShutdownLogger
{
   private static final Logger log = Logger.getLogger(JDBCShutdownLogger.class); 
   
   // Constructors ----------------------------------------------------
   
   public JDBCShutdownLogger(DataSource ds, TransactionManager tm, Properties sqlProperties,
                             boolean createTablesOnStartup)
   {
      super(ds, tm, sqlProperties, createTablesOnStartup);
   }
   
   // ShutdownLogger implementation ---------------------------------------------
   
   public boolean shutdown(String nodeId) throws Exception
   {
      boolean exists = existsStartup(nodeId);
      
      if (!exists)
      {
         //This shouldn't really happen
         log.warn("It appears the server did not start up properly!");
         return false;
      }
      else
      {      
         removeStartup(nodeId);
         
         return true;
      }      
   }

   public boolean startup(String nodeId) throws Exception
   {
      boolean crashed = existsStartup(nodeId);
      
      if (crashed)
      {
         //These means the server didn't shutdown cleanly last time - 
         //we must assume it crashed
         
         log.warn("It appears the server was not shutdown cleanly last time. We assume that the server crashed");
         
         //Remove the old startup row
         removeStartup(nodeId);
      }
      
      //Insert a new startup row
      insertStartup(nodeId);
      
      return crashed;
   }
   
   private boolean existsStartup(String nodeId) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         ps = conn.prepareStatement(getSQLStatement("SELECT_STARTUP"));
         ps.setString(1, nodeId);
         
         rs = ps.executeQuery();
         
         return rs.next();
        
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   private void removeStartup(String nodeId) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         ps = conn.prepareStatement(getSQLStatement("DELETE_STARTUP"));
         ps.setString(1, nodeId);
         
         ps.executeUpdate();
        
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   // Private ------------------------------------------------------------
   
   private void insertStartup(String nodeId) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         ps = conn.prepareStatement(getSQLStatement("INSERT_STARTUP"));
         ps.setString(1, nodeId);
         
         ps.executeUpdate();
        
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   protected Map getDefaultDDLStatements()
   {
      Map sql = new LinkedHashMap();
      
      sql.put("CREATE_STARTUP", "CREATE TABLE JMS_STARTUP (NODE_ID VARCHAR(255) PRIMARY KEY)");
      
      return sql;
   }

   protected Map getDefaultDMLStatements()
   {
      Map sql = new LinkedHashMap();
      
      sql.put("SELECT_STARTUP", "SELECT NODE_ID FROM JMS_STARTUP WHERE NODE_ID = ?");
      sql.put("DELETE_STARTUP", "DELETE FROM JMS_STARTUP WHERE NODE_ID = ?");
      sql.put("INSERT_STARTUP", "INSERT INTO JMS_STARTUP (NODE_ID) VALUES (?)");
      
      return sql;
   }

}

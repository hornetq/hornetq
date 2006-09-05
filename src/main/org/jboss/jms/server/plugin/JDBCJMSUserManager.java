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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.jms.server.plugin.contract.JMSUserManager;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.JDBCServiceSupport;

/**
 * A JDBCJMSUserManager
 * 
 * Manages JMS user and role data - in particular the predefined client id (if any)
 * for the user
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class JDBCJMSUserManager extends JDBCServiceSupport implements JMSUserManager
{
   private static final Logger log = Logger.getLogger(JDBCJMSUserManager.class);
      
   // PersistentServiceSupport overrides ----------------------------
   
   protected Map getDefaultDMLStatements()
   {                
      Map map = new HashMap();
      map.put("SELECT_PRECONF_CLIENTID", "SELECT CLIENTID FROM JMS_USER WHERE USERID=?");
      return map;
   }
   
   protected Map getDefaultDDLStatements()
   {
      Map map = new HashMap();
      map.put("CREATE_USER_TABLE",
              "CREATE TABLE JMS_USER (USERID VARCHAR(32) NOT NULL, PASSWD VARCHAR(32) NOT NULL, CLIENTID VARCHAR(128)," +
              " PRIMARY KEY(USERID))");
      map.put("CREATE_ROLE_TABLE",
              "CREATE TABLE JMS_ROLE (ROLEID VARCHAR(32) NOT NULL, USERID VARCHAR(32) NOT NULL," +
              " PRIMARY KEY(USERID, ROLEID))");
      return map;
   }
   
   // ServiceMBeanSupport overrides ---------------------------------
   
   protected void startService() throws Exception
   {
      super.startService();
      
      insertUserRoleData();
   }

   // MBean operations ----------------------------------------------
   
   // MBean attributes --------------------------------------------------------
   
   // JMSUserManager implementation -------------------------------------

   //TODO There is no reason why this class should implement ClientIDRetriever
   //the implementation should be moved out into a completely different service
   public String getPreConfiguredClientID(String username) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps  = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(getSQLStatement("SELECT_PRECONF_CLIENTID"));
         
         ps.setString(1, username);
         
         rs = ps.executeQuery();
         
         String clientID = null;
         
         if (rs.next())
         {
            clientID = rs.getString(1);
         }
         
         return clientID;
      }
      catch (SQLException e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (rs != null)
         {
            rs.close();
         }
         if (ps != null)
         {
            ps.close();
         }
         if (conn != null)
         {
            conn.close();
         }
         wrap.end();
      }     
   }
   
   private void insertUserRoleData() throws Exception
   {
      List populateTables = new ArrayList();
      for (Iterator i = sqlProperties.entrySet().iterator(); i.hasNext();)
      {
         Map.Entry entry = (Map.Entry) i.next();
         String key = (String) entry.getKey();
         if (key.startsWith("POPULATE.TABLES."))
         {
            populateTables.add(entry.getValue());
         }
      }      
      
      if (!populateTables.isEmpty())
      {         
         Connection conn = null;      
         TransactionWrapper tx = new TransactionWrapper();
         
         try
         {
            conn = ds.getConnection();
            
            Iterator iter = populateTables.iterator();
            
            while (iter.hasNext())
            {
               String statement = (String)iter.next();
               
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
   }
   
 
   // Inner classes -------------------------------------------------      
}

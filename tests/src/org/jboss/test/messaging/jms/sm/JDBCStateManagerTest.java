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
package org.jboss.test.messaging.jms.sm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.Set;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.JDBCStateManager;
import org.jboss.jms.server.StateManager;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.tm.TransactionManagerService;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JDBCStateManagerTest extends InMemoryStateManagerTest
{
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public JDBCStateManagerTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();      
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   protected StateManager createStateManager() throws Exception
   {
      JDBCStateManager sm = new JDBCStateManager(ServerManagement.getServerPeer());
      sm.start();
      return sm;
   }
   
   public void testGetPreConfClientId() throws Exception
   {
      InitialContext ctx = new InitialContext();
      TransactionManager mgr = (TransactionManager) ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      String username = new GUID().toString();
      String clientID = new GUID().toString();
      String password = new GUID().toString();
      Transaction txOld = mgr.suspend();
      mgr.begin();
      Connection conn = ds.getConnection();
      String sql = "INSERT INTO JMS_USER (USERID, CLIENTID, PASSWD) VALUES (?,?,?)";
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setString(1, username);
      ps.setString(2, clientID);
      ps.setString(3, password);
      int rows = ps.executeUpdate();
      assertEquals(1, rows);
      mgr.commit();
      if (txOld != null)
      {
         mgr.resume(txOld);
      }
      conn.close();
      String theClientID = sm.getPreConfiguredClientID(username);
      assertNotNull(theClientID);
      assertEquals(clientID, theClientID);
           
   }
   
   public void testLoadDurableSubscriptionsForTopic() throws Exception
   {
      final int NUM_SUBS = 10;
      
      ServerManagement.getServerPeer().getDestinationManager().createTopic("topic1");
      
      ServerManagement.getServerPeer().getDestinationManager().createTopic("topic2");
      
      DurableSubscription[] subs = new DurableSubscription[NUM_SUBS];
      
      for (int i = 0; i < NUM_SUBS; i++)
      {
         subs[i] =
            sm.createDurableSubscription("topic1", new GUID().toString(), new GUID().toString(), new GUID().toString());         
         sm.createDurableSubscription("topic2", new GUID().toString(), new GUID().toString(), new GUID().toString());
      }
      
      Set loaded = sm.loadDurableSubscriptionsForTopic("topic1");
      assertNotNull(loaded);
      assertEquals(NUM_SUBS, loaded.size());
      
      for (int i = 0; i < NUM_SUBS; i++)
      {
         Iterator iter = loaded.iterator();
         boolean found = false;
         while (iter.hasNext())
         {
            DurableSubscription subloaded = (DurableSubscription)iter.next();            
            if (subloaded.getChannelID().equals(subs[i].getChannelID()))
            {
               assertEquals(subloaded.getSubName(), subs[i].getSubName());
               assertEquals(subloaded.getSelector(), subs[i].getSelector());
               assertEquals(subloaded.getTopic().getName(), "topic1");
               found = true;
               break;
            }
         }
         if (!found)
         {
            fail();
         }
      }
       
   }
  
}





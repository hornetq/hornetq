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
package org.jboss.test.messaging.jms.server.plugin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.Set;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.plugin.contract.DurableSubscriptionStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.jms.server.DestinationManager;
import org.jboss.messaging.core.local.CoreDurableSubscription;
import org.jboss.messaging.core.persistence.JDBCUtil;
import org.jboss.messaging.core.plugin.contract.TransactionLog;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.tm.TransactionManagerService;
import org.jboss.util.id.GUID;


/**
 * These tests must not be ran in remote mode!
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JDBCDurableSubscriptionStoreTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------

   protected TransactionLog tl;
   protected DestinationManager dm;
   protected MessageStore ms;
   protected DurableSubscriptionStore dssd;

   // Constructors --------------------------------------------------

   public JDBCDurableSubscriptionStoreTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("This test must not be ran in remote mode!");
      }

      super.setUp();

      ServerManagement.start("all");

      dm = ServerManagement.getDestinationManager();
      ms = ServerManagement.getMessageStore();
      dssd = ServerManagement.getDurableSubscriptionStore();

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      log.debug("starting tear down");
      super.tearDown();
   }

   public void testCreateGetRemoveDurableSubscription() throws Exception
   {
      String topicName = new GUID().toString();
      String clientID = new GUID().toString();
      String subscriptionName = new GUID().toString();
      String selector = new GUID().toString();

      ServerManagement.deployTopic(topicName);

      CoreDurableSubscription sub = dssd.createDurableSubscription(topicName,
                                                               clientID,
                                                               subscriptionName,
                                                               selector,
                                                               false,
                                                               dm, ms, tl);

      assertEquals(sub.getTopic().getName(), topicName);
      assertEquals(sub.getChannelID(), clientID + "." + subscriptionName);
      assertEquals(sub.getSelector(), selector);

      CoreDurableSubscription sub_r = dssd.getDurableSubscription(clientID,
                                                              subscriptionName,
                                                              dm, ms, tl);

      assertEquals(sub_r.getTopic().getName(), topicName);
      assertEquals(sub_r.getChannelID(), clientID + "." + subscriptionName);
      assertEquals(sub_r.getSelector(), selector);

      boolean removed = dssd.removeDurableSubscription(clientID, subscriptionName);
      assertTrue(removed);

      sub_r = dssd.getDurableSubscription(clientID, subscriptionName, dm, ms, tl);

      assertNull(sub_r);

      removed = dssd.removeDurableSubscription(clientID, subscriptionName);
      assertFalse(removed);

   }

   public void testCreateGetRemoveDurableSubscriptionNullSelector() throws Exception
   {
      String topicName = new GUID().toString();
      String clientID = new GUID().toString();
      String subscriptionName = new GUID().toString();

      ServerManagement.deployTopic(topicName);

      CoreDurableSubscription sub = dssd.createDurableSubscription(topicName,
                                                               clientID,
                                                               subscriptionName,
                                                               null,
                                                               false,
                                                               dm, ms, tl);

      assertEquals(sub.getTopic().getName(), topicName);
      assertEquals(sub.getChannelID(), clientID + "." + subscriptionName);
      assertNull(sub.getSelector());

      CoreDurableSubscription sub_r = dssd.getDurableSubscription(clientID,
                                                              subscriptionName,
                                                              dm, ms, tl);

      assertEquals(sub_r.getTopic().getName(), topicName);
      assertEquals(sub_r.getChannelID(), clientID + "." + subscriptionName);
      assertNull(sub_r.getSelector());

      boolean removed = dssd.removeDurableSubscription(clientID, subscriptionName);
      assertTrue(removed);

      sub_r = dssd.getDurableSubscription(clientID, subscriptionName, dm, ms, tl);

      assertNull(sub_r);

      removed = dssd.removeDurableSubscription(clientID, subscriptionName);
      assertFalse(removed);

   }

   public void testGetPreConfClientId_1() throws Exception
   {
      String clientID = dssd.getPreConfiguredClientID("blahblah");
      assertNull(clientID);
   }

   public void testGetPreConfClientId_2() throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
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

      log.debug(JDBCUtil.statementToString(sql, username, clientID, password) + " completed successfully");

      assertEquals(1, rows);

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }

      conn.close();

      String theClientID = dssd.getPreConfiguredClientID(username);

      assertNotNull(theClientID);
      assertEquals(clientID, theClientID);
           
   }
   
   public void testLoadDurableSubscriptionsForTopic() throws Exception
   {
      final int NUM_SUBS = 10;
      
      ServerManagement.deployTopic("topic1");
      ServerManagement.deployTopic("topic2");
      
      CoreDurableSubscription[] subs = new CoreDurableSubscription[NUM_SUBS];
      
      for (int i = 0; i < NUM_SUBS; i++)
      {
         subs[i] = dssd.createDurableSubscription("topic1",
                                                  new GUID().toString(),
                                                  new GUID().toString(),
                                                  new GUID().toString(),
                                                  false,
                                                  dm, ms, tl);
         dssd.createDurableSubscription("topic2",
                                        new GUID().toString(),
                                        new GUID().toString(),
                                        new GUID().toString(),
                                        false,
                                        dm, ms, tl);
      }
      
      Set loaded = dssd.loadDurableSubscriptionsForTopic("topic1", dm, ms, tl);
      assertNotNull(loaded);
      assertEquals(NUM_SUBS, loaded.size());
      
      for (int i = 0; i < NUM_SUBS; i++)
      {
         Iterator iter = loaded.iterator();
         boolean found = false;
         while (iter.hasNext())
         {
            CoreDurableSubscription subloaded = (CoreDurableSubscription)iter.next();
            if (subloaded.getChannelID().equals(subs[i].getChannelID()))
            {
               assertEquals(subloaded.getName(), subs[i].getName());
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}





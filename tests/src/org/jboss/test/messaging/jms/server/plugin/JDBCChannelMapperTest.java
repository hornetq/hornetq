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

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.plugin.contract.ChannelMapper;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.local.CoreDurableSubscription;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.persistence.JDBCUtil;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.tm.TransactionManagerService;
import org.jboss.util.id.GUID;

/**
 * These tests must not be run in remote mode!
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * JDBCChannelMapperTest.java,v 1.1 2006/02/28 16:48:15 timfox Exp
 */
public class JDBCChannelMapperTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------

   protected PersistenceManager pm;
   protected DestinationManager dm;
   protected MessageStore ms;
   protected ChannelMapper channelMapper;

   // Constructors --------------------------------------------------

   public JDBCChannelMapperTest(String name)
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
      channelMapper = ServerManagement.getChannelMapper();

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

      CoreDurableSubscription sub = channelMapper.createDurableSubscription(topicName,
                                                               clientID,
                                                               subscriptionName,
                                                               selector,
                                                               false,
                                                               ms, pm);

      assertEquals(sub.getSelector(), selector);

      CoreDurableSubscription sub_r = channelMapper.getDurableSubscription(clientID,
                                                              subscriptionName,
                                                              ms, pm);

      assertEquals(sub_r.getSelector(), selector);

      boolean removed = channelMapper.removeDurableSubscription(clientID, subscriptionName);
      assertTrue(removed);

      sub_r = channelMapper.getDurableSubscription(clientID, subscriptionName, ms, pm);

      assertNull(sub_r);

      removed = channelMapper.removeDurableSubscription(clientID, subscriptionName);
      assertFalse(removed);

   }

   public void testCreateGetRemoveDurableSubscriptionNullSelector() throws Exception
   {
      String topicName = new GUID().toString();
      String clientID = new GUID().toString();
      String subscriptionName = new GUID().toString();

      ServerManagement.deployTopic(topicName);

      CoreDurableSubscription sub = channelMapper.createDurableSubscription(topicName,
                                                               clientID,
                                                               subscriptionName,
                                                               null,
                                                               false,
                                                               ms, pm);

      assertNull(sub.getSelector());

      CoreDurableSubscription sub_r = channelMapper.getDurableSubscription(clientID,
                                                              subscriptionName,
                                                              ms, pm);

      assertNull(sub_r.getSelector());

      boolean removed = channelMapper.removeDurableSubscription(clientID, subscriptionName);
      assertTrue(removed);

      sub_r = channelMapper.getDurableSubscription(clientID, subscriptionName, ms, pm);

      assertNull(sub_r);

      removed = channelMapper.removeDurableSubscription(clientID, subscriptionName);
      assertFalse(removed);

   }

   public void testGetPreConfClientId_1() throws Exception
   {
      String clientID = channelMapper.getPreConfiguredClientID("blahblah");
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

      String theClientID = channelMapper.getPreConfiguredClientID(username);

      assertNotNull(theClientID);
      assertEquals(clientID, theClientID);
           
   }
   
   public void testGetDeployCoreDestinationTest() throws Exception
   {
      //Lookup a non existent core destination -  verify returns null
      
      JBossQueue queue = new JBossQueue("queue1");
      
      JBossTopic topic = new JBossTopic("topic1");
      
      CoreDestination cd = channelMapper.getCoreDestination(queue);
      
      assertNull(cd);
      
      cd = channelMapper.getCoreDestination(topic);
      
      assertNull(cd);
      
      //Lookup a non existent jboss destination - verify returns null
      
      JBossDestination jbd = channelMapper.getJBossDestination(123);
      
      assertNull(jbd);
      
      //Deploy core destinations
      
      channelMapper.deployCoreDestination(true, "queue1", ms, pm);
      
      channelMapper.deployCoreDestination(false, "topic1", ms, pm);
      
      //Lookup core dest
      
      CoreDestination cd1 = channelMapper.getCoreDestination(queue);
      
      assertNotNull(cd1);
      
      assertTrue(cd1 instanceof Queue);
      
      Queue q = (Queue)cd1;
      
      CoreDestination cd2 = channelMapper.getCoreDestination(topic);
      
      assertNotNull(cd2);
      
      assertTrue(cd2 instanceof Topic);
      
      Topic t = (Topic)cd2;
                  
      //Lookup jboss dest
      
      JBossDestination jb1 = channelMapper.getJBossDestination(q.getChannelID());
      
      assertNotNull(jb1);
      
      assertEquals("queue1", jb1.getName());
      
      JBossDestination jb2 = channelMapper.getJBossDestination(t.getId());
      
      assertNotNull(jb2);
      
      assertEquals("topic1", jb2.getName());
      
      //undeploy core dest
      
      CoreDestination cd3 = channelMapper.undeployCoreDestination(true, "queue1");
      
      assertNotNull(cd3);
      
      assertEquals(q.getChannelID(), cd3.getId());
      
      CoreDestination cd4 = channelMapper.undeployCoreDestination(false, "topic1");
      
      assertNotNull(cd3);
      
      assertEquals(t.getId(), cd4.getId());
      
      cd3 = channelMapper.undeployCoreDestination(true, "queue1");
      
      assertNull(cd3);
      
      cd4 = channelMapper.undeployCoreDestination(false, "topic1");
      
      assertNull(cd4);
      
      //Lookup core dest - null
      
      cd3 = channelMapper.getCoreDestination(queue);
      
      assertNull(cd3);
      
      cd4 = channelMapper.getCoreDestination(topic);
      
      assertNull(cd4);
      
      //lookup jboss dest - null
      
      jb1 = channelMapper.getJBossDestination(q.getChannelID());
      
      assertNull(jb1);
      
      jb2 = channelMapper.getJBossDestination(t.getId());
      
      assertNull(jb2);
      
      //Deploy a core dest
      
      channelMapper.deployCoreDestination(true, "queue1", ms, pm);
      
      channelMapper.deployCoreDestination(false, "topic1", ms, pm);
            
      //lookup core dest - verify has same id
      
      cd3 = channelMapper.getCoreDestination(queue);
      
      assertNotNull(cd3);
      
      assertEquals(cd3.getId(), cd1.getId());
      
      cd4 = channelMapper.getCoreDestination(topic);
      
      assertNotNull(cd4);
      
      assertEquals(cd4.getId(), cd2.getId());
   }
   
   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}





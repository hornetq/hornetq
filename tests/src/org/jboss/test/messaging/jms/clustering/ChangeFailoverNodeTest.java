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

package org.jboss.test.messaging.jms.clustering;

import java.util.Iterator;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>8 Jul 2007
 * 
 * $Id: $
 * 
 */
public class ChangeFailoverNodeTest extends ClusteringTestBase
{

   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String queueName = "testDistributedQueue";

   // Constructors --------------------------------------------------

   public ChangeFailoverNodeTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testKillFailoverNodeTransactional() throws Exception
   {
      this.killFailoverNode(true);
   }

   public void testKillFailoverNodeNonTransactional() throws Exception
   {
      this.killFailoverNode(false);
   }

   public void testStopFailoverNodeTransactional() throws Exception
   {
      this.stopFailoverNode(true);
   }

   public void testStopFailoverNodeNonTransactional() throws Exception
   {
      this.stopFailoverNode(false);
   }

   public void testAddNodeToGetNewFailoverNodeNonTransactional()
         throws Exception
   {
      this.addNodeToGetNewFailoverNode(false);
   }

   public void testAddNodeToGetNewFailoverNodeTransactional() throws Exception
   {
      this.addNodeToGetNewFailoverNode(true);
   }

   public void testKillAllToOneAndBackAgainNonTransactional() throws Exception
   {
      this.killAllToOneAndBackAgain(false);
   }

   public void testKillAllToOneAndBackAgainTransactional() throws Exception
   {
      this.killAllToOneAndBackAgain(true);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void killAllToOneAndBackAgain(boolean transactional)
         throws Exception
   {
      Connection conn1 = createConnectionOnServer(cf, 1);

      try
      {
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection) conn1).registerFailoverListener(failoverListener);

         Session sessSend = conn1
               .createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod1 = sessSend.createProducer(queue[1]);

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);

            prod1.send(tm);
         }

         sessSend.close();

         Session sess1 = conn1.createSession(transactional,
               transactional ? Session.SESSION_TRANSACTED
                     : Session.CLIENT_ACKNOWLEDGE);

         MessageConsumer cons1 = sess1.createConsumer(queue[1]);

         conn1.start();

         TextMessage tm = null;

         for (int i = 0; i < numMessages; i++)
         {
            tm = (TextMessage) cons1.receive(2000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }

         // Don't ack

         int failoverNodeId = this.getFailoverNodeForNode(cf, 1);

         log.info("Failover node for node 1 is " + failoverNodeId);

         int recoveryMapSize = ServerManagement.getServer(failoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         Map recoveryArea = ServerManagement.getServer(failoverNodeId)
               .getRecoveryArea(queueName);
         Map ids = (Map) recoveryArea.get(new Integer(1));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         // Now kill/stop the failover node

         log.info("killing/stoppin node " + failoverNodeId);
         if (failoverNodeId != 0)
         {
            ServerManagement.kill(failoverNodeId);
         } else
         {
            ServerManagement.stop(failoverNodeId);
         }

         Thread.sleep(8000);

         int newFailoverNodeId = this.getFailoverNodeForNode(cf, 1);

         recoveryMapSize = ServerManagement.getServer(newFailoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(newFailoverNodeId)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(1));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         // Now kill the second failover node

         log.info("killing/stoppin node " + newFailoverNodeId);
         if (newFailoverNodeId != 0)
         {
            ServerManagement.kill(newFailoverNodeId);
         } else
         {
            ServerManagement.stop(newFailoverNodeId);
         }

         Thread.sleep(8000);

         int evennewerFailoverNodeId = this.getFailoverNodeForNode(cf, 1);

         recoveryMapSize = ServerManagement.getServer(evennewerFailoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(evennewerFailoverNodeId)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(1));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         // Now kill the third failover node

         log.info("killing/stoppin node " + evennewerFailoverNodeId);
         if (evennewerFailoverNodeId != 0)
         {
            ServerManagement.kill(evennewerFailoverNodeId);
         } else
         {
            ServerManagement.stop(evennewerFailoverNodeId);
         }

         // This just leaves the current node

         Thread.sleep(8000);

         int evenevennewerFailoverNodeId = this.getFailoverNodeForNode(cf, 1);

         assertEquals(1, evenevennewerFailoverNodeId);

         // Add a node

         ServerManagement.start(4, "all", false);

         ServerManagement.deployQueue("testDistributedQueue", 4);

         Thread.sleep(8000);

         log.info("started node 4");

         int evenevenevennewerFailoverNodeId = this.getFailoverNodeForNode(cf,
               1);

         recoveryMapSize = ServerManagement.getServer(
               evenevenevennewerFailoverNodeId).getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(
               evenevenevennewerFailoverNodeId).getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(1));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");

         while (true)
         {
            FailoverEvent event = failoverListener.getEvent(120000);
            if (event != null
                  && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }

         log.info("Failover completed");

         assertEquals(evenevenevennewerFailoverNodeId, getServerId(conn1));

         // Now ack
         if (transactional)
         {
            sess1.commit();
         } else
         {
            tm.acknowledge();
         }

         log.info("acked");

         sess1.close();

         log.info("closed");

         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         log.info("created new session");

         cons1 = sess1.createConsumer(queue[0]);

         log.info("Created consumer");

         // Messages should be gone

         tm = (TextMessage) cons1.receive(8000);

         assertNull(tm);

         recoveryMapSize = ServerManagement.getServer(
               evenevenevennewerFailoverNodeId).getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(
               evenevenevennewerFailoverNodeId).getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(1));
         assertNull(ids);
      } finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }

         ServerManagement.kill(4);
      }
   }

   private void addNodeToGetNewFailoverNode(boolean transactional)
         throws Exception
   {
      Connection conn = null;

      try
      {
         // First we must find the node that fails over onto zero since this is
         // the one that will change when
         // we add a node

         int nodeID = this.getNodeThatFailsOverOnto(cf, 0);

         conn = createConnectionOnServer(cf, nodeID);

         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection) conn).registerFailoverListener(failoverListener);

         Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod2 = sessSend.createProducer(queue[nodeID]);

         final int numMessages = 10;

         // Send some messages at this node

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);

            prod2.send(tm);
         }

         Session sess3 = conn.createSession(transactional,
               transactional ? Session.SESSION_TRANSACTED
                     : Session.CLIENT_ACKNOWLEDGE);

         MessageConsumer cons3 = sess3.createConsumer(queue[nodeID]);

         conn.start();

         TextMessage tm = null;

         for (int i = 0; i < numMessages; i++)
         {
            tm = (TextMessage) cons3.receive(2000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }

         // Don't ack

         int failoverNodeId = this.getFailoverNodeForNode(cf, nodeID);

         log.info("Failover node for node is " + failoverNodeId);

         dumpFailoverMap(ServerManagement.getServer(nodeID).getFailoverMap());

         int recoveryMapSize = ServerManagement.getServer(failoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         Map recoveryArea = ServerManagement.getServer(failoverNodeId)
               .getRecoveryArea(queueName);
         Map ids = (Map) recoveryArea.get(new Integer(nodeID));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         // We now add a new node - this should cause the failover node to
         // change

         ServerManagement.start(4, "all", false);

         ServerManagement.deployQueue("testDistributedQueue", 4);

         Thread.sleep(8000);

         dumpFailoverMap(ServerManagement.getServer(nodeID).getFailoverMap());

         int newFailoverNodeId = this.getFailoverNodeForNode(cf, nodeID);

         assertTrue(failoverNodeId != newFailoverNodeId);

         log.info("New failover node is " + newFailoverNodeId);

         recoveryMapSize = ServerManagement.getServer(failoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(failoverNodeId)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(nodeID));
         assertNull(ids);

         dumpFailoverMap(ServerManagement.getServer(nodeID).getFailoverMap());

         recoveryMapSize = ServerManagement.getServer(newFailoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(newFailoverNodeId)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(nodeID));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         // Now kill the node

         ServerManagement.kill(nodeID);

         log.info("########");
         log.info("######## KILLED NODE");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");

         while (true)
         {
            FailoverEvent event = failoverListener.getEvent(120000);
            if (event != null
                  && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }

         log.info("Failover completed");

         assertEquals(newFailoverNodeId, getServerId(conn));

         recoveryMapSize = ServerManagement.getServer(failoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(newFailoverNodeId)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(nodeID));
         assertNull(ids);

         // Now ack
         if (transactional)
         {
            sess3.commit();
         } else
         {
            tm.acknowledge();
         }

         log.info("acked");

         sess3.close();

         log.info("closed");

         sess3 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         log.info("created new session");

         cons3 = sess3.createConsumer(queue[nodeID]);

         log.info("Created consumer");

         // Messages should be gone

         tm = (TextMessage) cons3.receive(8000);

         assertNull(tm);
      } finally
      {
         if (conn != null)
         {
            conn.close();
         }

         ServerManagement.kill(4);
      }
   }

   private void dumpFailoverMap(Map map)
   {
      Iterator iter = map.entrySet().iterator();

      log.info("*** dumping failover map ***");

      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry) iter.next();

         log.info(entry.getKey() + "-->" + entry.getValue());
      }

      log.info("*** end dump ***");
   }

   private static int counter;

   private void killFailoverNode(boolean transactional) throws Exception
   {
      JBossConnectionFactory factory = (JBossConnectionFactory) ic[0]
            .lookup("/ClusteredConnectionFactory");

      Connection conn1 = createConnectionOnServer(factory, 1);

      try
      {
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection) conn1).registerFailoverListener(failoverListener);

         Session sessSend = conn1
               .createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod1 = sessSend.createProducer(queue[1]);

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);

            tm.setIntProperty("counter", counter++);

            prod1.send(tm);

            log.info("Sent " + tm.getJMSMessageID());
         }

         Session sess1 = conn1.createSession(transactional,
               transactional ? Session.SESSION_TRANSACTED
                     : Session.CLIENT_ACKNOWLEDGE);

         MessageConsumer cons1 = sess1.createConsumer(queue[1]);

         conn1.start();

         TextMessage tm = null;

         for (int i = 0; i < numMessages; i++)
         {
            tm = (TextMessage) cons1.receive(2000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }

         // Don't ack

         // We kill the failover node for node 1
         int failoverNodeId = this.getFailoverNodeForNode(factory, 1);

         int recoveryMapSize = ServerManagement.getServer(failoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         Map recoveryArea = ServerManagement.getServer(failoverNodeId)
               .getRecoveryArea(queueName);
         Map ids = (Map) recoveryArea.get(new Integer(1));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         log.info("Killing failover node:" + failoverNodeId);

         ServerManagement.kill(failoverNodeId);

         log.info("Killed failover node");

         Thread.sleep(8000);

         // Now kill node 1

         failoverNodeId = this.getFailoverNodeForNode(factory, 1);

         recoveryMapSize = ServerManagement.getServer(failoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(failoverNodeId)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(1));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         log.info("Failover node id is now " + failoverNodeId);

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");

         while (true)
         {
            FailoverEvent event = failoverListener.getEvent(30000);
            if (event != null
                  && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }

         log.info("Failover completed");

         assertEquals(failoverNodeId, getServerId(conn1));

         recoveryMapSize = ServerManagement.getServer(failoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(failoverNodeId)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(1));
         assertNull(ids);

         // Now ack
         if (transactional)
         {
            sess1.commit();
         } else
         {
            tm.acknowledge();
         }

         log.info("acked");

         sess1.close();

         log.info("closed");

         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         log.info("created new session");

         cons1 = sess1.createConsumer(queue[1]);

         log.info("Created consumer");

         // Messages should be gone

         tm = (TextMessage) cons1.receive(8000);

         assertNull(tm);
      } finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }
   }

   private void stopFailoverNode(boolean transactional) throws Exception
   {
      JBossConnectionFactory factory = (JBossConnectionFactory) ic[0]
            .lookup("/ClusteredConnectionFactory");

      Connection conn1 = createConnectionOnServer(factory, 1);

      try
      {
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection) conn1).registerFailoverListener(failoverListener);

         Session sessSend = conn1
               .createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod1 = sessSend.createProducer(queue[1]);

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);

            prod1.send(tm);

            log.info("Sent " + tm.getJMSMessageID());

         }

         Session sess1 = conn1.createSession(transactional,
               transactional ? Session.SESSION_TRANSACTED
                     : Session.CLIENT_ACKNOWLEDGE);

         MessageConsumer cons1 = sess1.createConsumer(queue[1]);

         conn1.start();

         TextMessage tm = null;

         for (int i = 0; i < numMessages; i++)
         {
            tm = (TextMessage) cons1.receive(2000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }

         // Don't ack

         // We stop the failover node for node 1
         int failoverNodeId = this.getFailoverNodeForNode(factory, 1);

         int recoveryMapSize = ServerManagement.getServer(failoverNodeId)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         Map recoveryArea = ServerManagement.getServer(failoverNodeId)
               .getRecoveryArea(queueName);
         Map ids = (Map) recoveryArea.get(new Integer(1));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         log.info("Stopping failover node:" + failoverNodeId);

         ServerManagement.stop(failoverNodeId);

         log.info("Stopped failover node");

         Thread.sleep(8000);

         int newfailoverNode = this.getFailoverNodeForNode(factory, 1);

         log.info("New failover node is " + newfailoverNode);

         recoveryMapSize = ServerManagement.getServer(newfailoverNode)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(newfailoverNode)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(1));
         assertNotNull(ids);
         assertEquals(numMessages, ids.size());

         Thread.sleep(8000);

         // Now kill node 1

         failoverNodeId = this.getFailoverNodeForNode(factory, 1);

         log.info("Failover node id is now " + failoverNodeId);

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         log.info("Waiting for failover to complete");

         while (true)
         {
            FailoverEvent event = failoverListener.getEvent(30000);
            if (event != null
                  && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }

         log.info("Failover completed");

         assertEquals(newfailoverNode, getServerId(conn1));

         recoveryMapSize = ServerManagement.getServer(newfailoverNode)
               .getRecoveryMapSize(queueName);
         assertEquals(0, recoveryMapSize);
         recoveryArea = ServerManagement.getServer(newfailoverNode)
               .getRecoveryArea(queueName);
         ids = (Map) recoveryArea.get(new Integer(1));

         log.info("Final failover");

         if (ids != null)
         {
            Iterator iter = ids.entrySet().iterator();
            while (iter.hasNext())
            {
               Map.Entry entry = (Map.Entry) iter.next();

               log.info(entry.getKey() + "--->" + entry.getValue());
            }
         }

         assertNull(ids);

         // Now ack
         if (transactional)
         {
            sess1.commit();
         } else
         {
            tm.acknowledge();
         }

         log.info("acked");

         sess1.close();

         log.info("closed");

         sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         log.info("created new session");

         cons1 = sess1.createConsumer(queue[1]);

         log.info("Created consumer");

         // Messages should be gone

         tm = (TextMessage) cons1.receive(8000);

         assertNull(tm);
      } finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }
   }

   // Inner classes -------------------------------------------------

}

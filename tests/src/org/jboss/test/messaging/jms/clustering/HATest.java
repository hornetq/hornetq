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

import java.util.Map;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.JBossMessageConsumer;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ConsumerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.message.MessageProxy;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class HATest extends ClusteringTestBase
{
   
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public HATest(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------
   
   //

   /**
    * This test was created as per http://jira.jboss.org/jira/browse/JBMESSAGING-685.
    */
   public void testEmptyCommit() throws Exception
   {
      Connection conn = cf.createConnection();
      JBossSession session = (JBossSession)conn.createSession(true, Session.SESSION_TRANSACTED);
      session.commit();
      conn.close();
   }

   /**
    * This test was created as per http://jira.jboss.org/jira/browse/JBMESSAGING-696.
    */
   public void testCloseOnFailover() throws Exception
   {
      JBossConnectionFactory factory = (JBossConnectionFactory) ic[0].lookup("/ClusteredConnectionFactory");

      Connection conn1 = createConnectionOnServer(factory,0);
      Connection conn2 = createConnectionOnServer(factory,1);
      Connection conn3 = createConnectionOnServer(factory,2);

      Connection[] conn = new Connection[]{conn1, conn2, conn3};
      
      this.checkConnectionsDifferentServers(conn);

      log.info("Connection delegate information after creation");

      for (int i = 0; i < conn.length; i++)
      {
         log.info("conn" + i + ".serverid=" + getServerId(conn[i]) + " conn" + i + ".ObjectID=" +
            getObjectId(conn[i]) + " locatorURL=" + getLocatorURL(conn[i]));
      }

      log.info("Killing server 1 and waiting 30 seconds for failover to kick in on client (from Lease)");
      ServerManagement.kill(1);
      Thread.sleep(30000);

      log.info("Connection delegate information after failover");

      for (int i = 0; i < conn.length; i++)
      {
         log.info("conn" + i + ".serverid=" + getServerId(conn[i]) + " conn" + i + ".ObjectID=" +
            getObjectId(conn[i]) + " locatorURL=" + getLocatorURL(conn[i]));
      }

      ConnectionState state2 = getConnectionState(conn2);
      ConnectionState state3 = getConnectionState(conn3);

      assertNotSame(state2.getRemotingConnection(), state3.getRemotingConnection());
      assertNotSame(state2.getRemotingConnection().getRemotingClient(),
                    state3.getRemotingConnection().getRemotingClient());

      conn1.close();

      assertNotNull(state2.getRemotingConnection());
      assertNotNull(state2.getRemotingConnection().getRemotingClient().getInvoker());
      assertTrue(state2.getRemotingConnection().getRemotingClient().getInvoker().isConnected());

      conn2.close();

      assertNotNull(state3.getRemotingConnection());
      assertNotNull(state3.getRemotingConnection().getRemotingClient().getInvoker());
      assertTrue(state3.getRemotingConnection().getRemotingClient().getInvoker().isConnected());

      log.info("Closing connection 3 now");

      // When I created the testcase this was failing, throwing exceptions. This was basically why
      // I created this testcase
      conn3.close();
   }

   /*
    * Test that connections created using a clustered connection factory are created round robin on
    * different servers
    */
   public void testRoundRobinConnectionCreation() throws Exception
   {
      JBossConnectionFactory factory =  (JBossConnectionFactory )ic[0].lookup("/ClusteredConnectionFactory");

      ClientClusteredConnectionFactoryDelegate delegate =
         (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

      log.info ("number of delegates = " + delegate.getDelegates().length);
      log.info ("number of servers = " + ServerManagement.getServer(0).getNodeIDView().size());

      assertEquals(3, delegate.getDelegates().length);

      ClientConnectionFactoryDelegate cf1 = delegate.getDelegates()[0];

      ClientConnectionFactoryDelegate cf2 = delegate.getDelegates()[1];

      ClientConnectionFactoryDelegate cf3 = delegate.getDelegates()[2];

      assertEquals(0, cf1.getServerID());

      assertEquals(1, cf2.getServerID());

      assertEquals(2, cf3.getServerID());

      assertEquals(3, ServerManagement.getServer(0).getNodeIDView().size());

      Connection conn1 = null;

      Connection conn2 = null;

      Connection conn3 = null;

      Connection conn4 = null;

      Connection conn5 = null;

      try
      {
         conn1 = createConnectionOnServer(factory, 0);  //server 0

         conn2 = createConnectionOnServer(factory, 1);  //server 1

         conn3 = createConnectionOnServer(factory, 2);  //server 2

         conn4 = createConnectionOnServer(factory, 0);  //server 0

         conn5 = createConnectionOnServer(factory, 1);  //server 1

         int serverID1 = getServerId(conn1);

         int serverID2 = getServerId(conn2);

         int serverID3 = getServerId(conn3);

         int serverID4 = getServerId(conn4);

         int serverID5 = getServerId(conn5);

         log.info("server id 1: " + serverID1);

         log.info("server id 2: " + serverID2);

         log.info("server id 3: " + serverID3);

         log.info("server id 4: " + serverID4);

         log.info("server id 5: " + serverID5);

         assertEquals(0, serverID1);

         assertEquals(1, serverID2);

         assertEquals(2, serverID3);

         assertEquals(0, serverID4);

         assertEquals(1, serverID5);
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }

         if (conn3 != null)
         {
            conn3.close();
         }

         if (conn4 != null)
         {
            conn4.close();
         }

         if (conn5 != null)
         {
            conn5.close();
         }
      }

   }

   /*
    * Test that the failover mapping is created correctly and updated properly when nodes leave
    * or join
    */
   public void testDefaultFailoverMap() throws Exception
   {
      {
         JBossConnectionFactory factory =  (JBossConnectionFactory )ic[0].lookup("/ClusteredConnectionFactory");

         ClientClusteredConnectionFactoryDelegate delegate =
            (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

         assertEquals(3, ServerManagement.getServer(0).getNodeIDView().size());

         ClientConnectionFactoryDelegate[] delegates = delegate.getDelegates();

         ClientConnectionFactoryDelegate cf1 = delegate.getDelegates()[0];

         ClientConnectionFactoryDelegate cf2 = delegate.getDelegates()[1];

         ClientConnectionFactoryDelegate cf3 = delegate.getDelegates()[2];

         //The order here depends on the order the servers were started in

         //If any servers get stopped and then started then the order will change

         log.info("cf1 serverid=" + cf1.getServerID());

         log.info("cf2 serverid=" + cf2.getServerID());

         log.info("cf3 serverid=" + cf3.getServerID());


         assertEquals(0, cf1.getServerID());

         assertEquals(1, cf2.getServerID());

         assertEquals(2, cf3.getServerID());

         Map failoverMap = delegate.getFailoverMap();

         assertEquals(3, delegates.length);

         assertEquals(3, failoverMap.size());

         // Default failover policy just chooses the node to the right

         assertEquals(cf2.getServerID(), ((Integer)failoverMap.get(new Integer(cf1.getServerID()))).intValue());

         assertEquals(cf3.getServerID(), ((Integer)failoverMap.get(new Integer(cf2.getServerID()))).intValue());

         assertEquals(cf1.getServerID(), ((Integer)failoverMap.get(new Integer(cf3.getServerID()))).intValue());
      }

      //Now cleanly stop one of the servers

      log.info("************** STOPPING SERVER 0");
      ServerManagement.stop(0);
      
      Thread.sleep(10000);

      log.info("server stopped");

      assertEquals(2, ServerManagement.getServer(1).getNodeIDView().size());

      {
         //Lookup another connection factory

         JBossConnectionFactory factory =  (JBossConnectionFactory )ic[1].lookup("/ClusteredConnectionFactory");

         log.info("Got connection factory");

         ClientClusteredConnectionFactoryDelegate delegate =
            (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

         ClientConnectionFactoryDelegate[] delegates = delegate.getDelegates();

         Map failoverMap = delegate.getFailoverMap();

         log.info("Got failover map");

         assertEquals(2, delegates.length);

         ClientConnectionFactoryDelegate cf1 = delegate.getDelegates()[0];

         ClientConnectionFactoryDelegate cf2 = delegate.getDelegates()[1];

         //Order here depends on order servers were started in

         log.info("cf1 serverid=" + cf1.getServerID());

         log.info("cf2 serverid=" + cf2.getServerID());

         assertEquals(1, cf1.getServerID());

         assertEquals(2, cf2.getServerID());


         assertEquals(2, failoverMap.size());

         assertEquals(cf2.getServerID(), ((Integer)failoverMap.get(new Integer(cf1.getServerID()))).intValue());

         assertEquals(cf1.getServerID(), ((Integer)failoverMap.get(new Integer(cf2.getServerID()))).intValue());
      }

      //Cleanly stop another server

      log.info("Server 1 is started: " + ServerManagement.getServer(1).isServerPeerStarted());

      ServerManagement.stop(1);
      
      Thread.sleep(10000);

      assertEquals(1, ServerManagement.getServer(2).getNodeIDView().size());

      {
         //Lookup another connection factory

         JBossConnectionFactory factory =  (JBossConnectionFactory )ic[2].lookup("/ClusteredConnectionFactory");

         ClientClusteredConnectionFactoryDelegate delegate =
            (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

         ClientConnectionFactoryDelegate[] delegates = delegate.getDelegates();

         Map failoverMap = delegate.getFailoverMap();

         assertEquals(1, delegates.length);

         ClientConnectionFactoryDelegate cf1 = delegate.getDelegates()[0];

         assertEquals(2, cf1.getServerID());


         assertEquals(1, failoverMap.size());

         assertEquals(cf1.getServerID(), ((Integer)failoverMap.get(new Integer(cf1.getServerID()))).intValue());
      }

      //Restart server 0

      ServerManagement.start(0, "all");

      {
         JBossConnectionFactory factory =  (JBossConnectionFactory )ic[0].lookup("/ClusteredConnectionFactory");

         log.info("Got connection factory");

         ClientClusteredConnectionFactoryDelegate delegate =
            (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

         ClientConnectionFactoryDelegate[] delegates = delegate.getDelegates();

         Map failoverMap = delegate.getFailoverMap();

         log.info("Got failover map");

         assertEquals(2, delegates.length);

         ClientConnectionFactoryDelegate cf1 = delegate.getDelegates()[0];

         ClientConnectionFactoryDelegate cf2 = delegate.getDelegates()[1];

         log.info("cf1 serverid=" + cf1.getServerID());

         log.info("cf2 serverid=" + cf2.getServerID());

         assertEquals(0, cf1.getServerID());

         assertEquals(2, cf2.getServerID());


         assertEquals(2, failoverMap.size());

         assertEquals(cf2.getServerID(), ((Integer)failoverMap.get(new Integer(cf1.getServerID()))).intValue());

         assertEquals(cf1.getServerID(), ((Integer)failoverMap.get(new Integer(cf2.getServerID()))).intValue());
      }


      //Restart server 1

      ServerManagement.start(1, "all");

      {
         JBossConnectionFactory factory =  (JBossConnectionFactory )ic[1].lookup("/ClusteredConnectionFactory");

         log.info("Got connection factory");

         ClientClusteredConnectionFactoryDelegate delegate =
            (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

         ClientConnectionFactoryDelegate[] delegates = delegate.getDelegates();

         Map failoverMap = delegate.getFailoverMap();

         log.info("Got failover map");

         assertEquals(3, delegates.length);

         ClientConnectionFactoryDelegate cf1 = delegate.getDelegates()[0];

         ClientConnectionFactoryDelegate cf2 = delegate.getDelegates()[1];

         ClientConnectionFactoryDelegate cf3 = delegate.getDelegates()[2];

         log.info("cf1 serverid=" + cf1.getServerID());

         log.info("cf2 serverid=" + cf2.getServerID());

         log.info("cf3 serverid=" + cf3.getServerID());

         assertEquals(0, cf1.getServerID());

         assertEquals(1, cf2.getServerID());

         assertEquals(2, cf3.getServerID());


         assertEquals(3, failoverMap.size());

         assertEquals(cf2.getServerID(), ((Integer)failoverMap.get(new Integer(cf1.getServerID()))).intValue());

         assertEquals(cf3.getServerID(), ((Integer)failoverMap.get(new Integer(cf2.getServerID()))).intValue());

         assertEquals(cf1.getServerID(), ((Integer)failoverMap.get(new Integer(cf3.getServerID()))).intValue());
      }
   }

   public void testSimpleFailover() throws Exception
   {
      JBossConnectionFactory factory =  (JBossConnectionFactory )ic[0].lookup("/ClusteredConnectionFactory");

      ClientClusteredConnectionFactoryDelegate delegate =
         (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

      Set nodeIDView = ServerManagement.getServer(0).getNodeIDView();
      assertEquals(3, nodeIDView.size());

      ClientConnectionFactoryDelegate[] delegates = delegate.getDelegates();

      ClientConnectionFactoryDelegate cf1 = delegates[0];

      ClientConnectionFactoryDelegate cf2 = delegates[1];

      ClientConnectionFactoryDelegate cf3 = delegates[2];

      int server0Id = cf1.getServerID();

      int server1Id = cf2.getServerID();

      int server2Id = cf3.getServerID();

      log.info("server 0 id: " + server0Id);

      log.info("server 1 id: " + server1Id);

      log.info("server 2 id: " + server2Id);

      Map failoverMap = delegate.getFailoverMap();

      log.info(failoverMap.get(new Integer(server0Id)));
      log.info(failoverMap.get(new Integer(server1Id)));
      log.info(failoverMap.get(new Integer(server2Id)));

      int server1FailoverId = ((Integer)failoverMap.get(new Integer(server1Id))).intValue();

      // server 1 should failover onto server 2

      assertEquals(server2Id, server1FailoverId);

      Connection conn = null;

      try
      {
         conn = createConnectionOnServer(factory, 1);

         assertEquals(1, getServerId(conn));

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue[1]);

         MessageConsumer cons = sess.createConsumer(queue[1]);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("message:" + i);

            prod.send(tm);
         }

         //So now, messages should be in queue[1] on server 1
         //So we now kill server 1
         //Which should cause transparent failover of connection conn onto server 1

         log.info("######");
         log.info("###### KILLING (CRASHING) SERVER 1");
         log.info("######");

         ServerManagement.kill(1);

         long sleepTime = 30;

         log.info("killed server, now waiting for " + sleepTime + " seconds");

         // NOTE: the sleep time needs to be longer than the Remoting connector's lease period
         Thread.sleep(sleepTime * 1000);

         log.info("done wait");

         int finalServerID = getServerId(conn);

         log.info("final server id= " + finalServerID);

         //server id should now be 2

         assertEquals(2, finalServerID);

         conn.start();

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(1000);

            log.debug("message is " + tm);

            assertNotNull(tm);

            assertEquals("message:" + i, tm.getText());
         }
         log.info("done");
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }

         ServerManagement.start(1, "all");
      }
   }


   public void testFailoverWithUnackedMessagesClientAcknowledge() throws Exception
   {
      JBossConnectionFactory factory =  (JBossConnectionFactory )ic[0].lookup("/ClusteredConnectionFactory");

      ClientClusteredConnectionFactoryDelegate delegate =
         (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

      Set nodeIDView = ServerManagement.getServer(0).getNodeIDView();
      assertEquals(3, nodeIDView.size());

      ClientConnectionFactoryDelegate[] delegates = delegate.getDelegates();

      ClientConnectionFactoryDelegate cf1 = delegates[0];

      ClientConnectionFactoryDelegate cf2 = delegates[1];

      ClientConnectionFactoryDelegate cf3 = delegates[2];

      int server0Id = cf1.getServerID();

      int server1Id = cf2.getServerID();

      int server2Id = cf3.getServerID();

      log.info("server 0 id: " + server0Id);

      log.info("server 1 id: " + server1Id);

      log.info("server 2 id: " + server2Id);

      assertEquals(0, server0Id);

      assertEquals(1, server1Id);

      assertEquals(2, server2Id);

      Map failoverMap = delegate.getFailoverMap();

      log.info(failoverMap.get(new Integer(server0Id)));
      log.info(failoverMap.get(new Integer(server1Id)));
      log.info(failoverMap.get(new Integer(server2Id)));

      int server1FailoverId = ((Integer)failoverMap.get(new Integer(server1Id))).intValue();

      // server 1 should failover onto server 2

      assertEquals(server2Id, server1FailoverId);

      Connection conn = null;

      boolean killed = false;

      try
      {
         conn = createConnectionOnServer(factory, 1);

         JBossConnection jbc = (JBossConnection)conn;

         ClientConnectionDelegate del = (ClientConnectionDelegate)jbc.getDelegate();

         ConnectionState state = (ConnectionState)del.getState();

         int initialServerID = state.getServerID();

         assertEquals(1, initialServerID);

         Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue[1]);

         MessageConsumer cons = sess.createConsumer(queue[1]);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("message:" + i);

            prod.send(tm);
         }

         conn.start();

         //Now consume half of the messages but don't ack them these will end up in
         //client side toAck list

         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(500);

            assertNotNull(tm);

            assertEquals("message:" + i, tm.getText());
         }

         //So now, messages should be in queue[1] on server 1
         //So we now kill server 1
         //Which should cause transparent failover of connection conn onto server 1

         log.info("here we go");
         log.info("######");
         log.info("###### KILLING (CRASHING) SERVER 1");
         log.info("######");

         ServerManagement.kill(1);

         killed = true;

         long sleepTime = 30;

         log.info("killed server, now waiting for " + sleepTime + " seconds");

         // NOTE: the sleep time needs to be longer than the Remoting connector's lease period
         Thread.sleep(sleepTime * 1000);

         log.info("done wait");

         state = (ConnectionState)del.getState();

         int finalServerID = state.getServerID();

         log.info("final server id= " + finalServerID);

         //server id should now be 2

         assertEquals(2, finalServerID);

         conn.start();

         //Now should be able to consume the rest of the messages

         log.info("here1");

         TextMessage tm = null;

         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            tm = (TextMessage)cons.receive(1000);

            assertNotNull(tm);

            log.debug("message is " + tm.getText());

            assertEquals("message:" + i, tm.getText());
         }

         log.info("here2");

         //Now should be able to acknowledge them

         tm.acknowledge();

         //Now check there are no more messages there
         sess.close();

         sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         cons = sess.createConsumer(queue[1]);

         Message m = cons.receive(500);

         assertNull(m);

         log.info("got to end of test");
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }

         // Resurrect dead server
         if (killed)
         {
            ServerManagement.start(1, "all");
         }
      }

   }

   
   /*
   TODO: Reactivate this test when http://jira.jboss.org/jira/browse/JBMESSAGING-883 is done
   public void testFailoverWithUnackedMessagesTransactional() throws Exception
   {
      JBossConnectionFactory factory =  (JBossConnectionFactory )ic[0].lookup("/ClusteredConnectionFactory");

      ClientClusteredConnectionFactoryDelegate delegate =
         (ClientClusteredConnectionFactoryDelegate)factory.getDelegate();

      Set nodeIDView = ServerManagement.getServer(0).getNodeIDView();
      assertEquals(3, nodeIDView.size());

      ClientConnectionFactoryDelegate[] delegates = delegate.getDelegates();

      ClientConnectionFactoryDelegate cf1 = delegates[0];

      ClientConnectionFactoryDelegate cf2 = delegates[1];

      ClientConnectionFactoryDelegate cf3 = delegates[2];

      int server0Id = cf1.getServerID();

      int server1Id = cf2.getServerID();

      int server2Id = cf3.getServerID();

      log.info("server 0 id: " + server0Id);

      log.info("server 1 id: " + server1Id);

      log.info("server 2 id: " + server2Id);

      Map failoverMap = delegate.getFailoverMap();

      log.info(failoverMap.get(new Integer(server0Id)));
      log.info(failoverMap.get(new Integer(server1Id)));
      log.info(failoverMap.get(new Integer(server2Id)));

      int server1FailoverId = ((Integer)failoverMap.get(new Integer(server1Id))).intValue();

      // server 1 should failover onto server 2

      assertEquals(server2Id, server1FailoverId);

      Connection conn = null;

      boolean killed = false;

      try
      {
         //Get a connection on server 1
         conn = factory.createConnection(); //connection on server 0

         conn.close();

         conn = factory.createConnection(); //connection on server 1

         JBossConnection jbc = (JBossConnection)conn;

         ClientConnectionDelegate del = (ClientConnectionDelegate)jbc.getDelegate();

         ConnectionState state = (ConnectionState)del.getState();

         int initialServerID = state.getServerID();

         assertEquals(1, initialServerID);

         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer prod = sess.createProducer(queue[1]);

         MessageConsumer cons = sess.createConsumer(queue[1]);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("message:" + i);

            prod.send(tm);
         }

         sess.commit();

         conn.start();

         //Now consume half of the messages but don't commit them these will end up in
         //client side resource manager

         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(500);

            assertNotNull(tm);

            assertEquals("message:" + i, tm.getText());
         }

         //So now, messages should be in queue[1] on server 1
         //So we now kill server 1
         //Which should cause transparent failover of connection conn onto server 1

         log.info("######");
         log.info("###### KILLING (CRASHING) SERVER 1");
         log.info("######");

         ServerManagement.kill(1);

         killed = true;

         log.info("killed server, now waiting");

         long sleepTime = 60;

         log.info("killed server, now waiting for " + sleepTime + " seconds");

         // NOTE: the sleep time needs to be longer than the Remoting connector's lease period
         Thread.sleep(sleepTime * 1000);

         log.info("done wait");

         state = (ConnectionState)del.getState();

         int finalServerID = state.getServerID();

         log.info("final server id= " + finalServerID);

         //server id should now be 2

         assertEquals(2, finalServerID);

         conn.start();

         //Now should be able to consume the rest of the messages

         log.info("here1");

         TextMessage tm = null;

         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            tm = (TextMessage)cons.receive(500);

            log.debug("message is " + tm.getText());

            assertNotNull(tm);

            assertEquals("message:" + i, tm.getText());
         }

         log.info("here2");

         //Now should be able to commit them

         sess.commit();

         //Now check there are no more messages there
         sess.close();

         sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         cons = sess.createConsumer(queue[1]);

         Message m = cons.receive(500);

         assertNull(m);

         log.info("got to end of test");
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }

         if (killed)
         {
            ServerManagement.start(1, "all");
         }
      }

   } */

   public void testTopicSubscriber() throws Exception
   {
      log.info("++testTopicSubscriber");

      log.info(">>Lookup Queue");
      
      Destination destination = (Destination) topic[1];

      JBossConnection conn = (JBossConnection)createConnectionOnServer(cf, 1);

      conn.setClientID("testClient");
      conn.start();

      try
      {

         JBossSession session = (JBossSession) conn.createSession(true, Session.SESSION_TRANSACTED);
         ClientSessionDelegate clientSessionDelegate = (ClientSessionDelegate) session.getDelegate();
         SessionState sessionState = (SessionState) clientSessionDelegate.getState();

         MessageConsumer consumerHA = session.createDurableSubscriber((Topic) destination, "T1");
         JBossMessageConsumer jbossConsumerHA = (JBossMessageConsumer) consumerHA;

         org.jboss.jms.client.delegate.ClientConsumerDelegate clientDelegate =
            (org.jboss.jms.client.delegate.ClientConsumerDelegate) jbossConsumerHA.getDelegate();
         ConsumerState consumerState = (ConsumerState) clientDelegate.getState();

         log.info("subscriptionName=" + consumerState.getSubscriptionName());

         log.info(">>Creating Producer");
         MessageProducer producer = session.createProducer(destination);
         log.info(">>creating Message");
         Message message = session.createTextMessage("Hello Before");
         log.info(">>sending Message");
         producer.send(message);
         session.commit();

         receiveMessage("consumerHA", consumerHA, true, false);

         session.commit();
         //if (true) return;

         Object txID = sessionState.getCurrentTxId();

         producer.send(session.createTextMessage("Hello again before failover"));

         ClientConnectionDelegate delegate = (ClientConnectionDelegate) conn.getDelegate();

         JMSRemotingConnection originalRemoting = delegate.getRemotingConnection();

         ServerManagement.kill(1);

         Thread.sleep(30000);
         // if failover happened, this object was replaced
         assertNotSame(originalRemoting, delegate.getRemotingConnection());

         message = session.createTextMessage("Hello After");
         log.info(">>Sending new message");
         producer.send(message);

         assertEquals(txID, sessionState.getCurrentTxId());
         log.info(">>Final commit");

         session.commit();

         log.info("Calling alternate receiver");
         receiveMessage("consumerHA", consumerHA, true, false);
         receiveMessage("consumerHA", consumerHA, true, false);
         receiveMessage("consumerHA", consumerHA, true, true);

         session.commit();
      }
      finally
      {
         if (conn!=null)
         {
            try { conn.close(); } catch (Throwable ignored) {}
         }
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void setUp() throws Exception
   {
      nodeCount = 3;

      super.setUp();

      log.debug("setup done");
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   // Private -------------------------------------------------------

   private void receiveMessage(String text, MessageConsumer consumer, boolean shouldAssert, boolean shouldBeNull) throws Exception
   {
      MessageProxy message = (MessageProxy) consumer.receive(3000);
      TextMessage txtMessage = (TextMessage) message;
      if (message != null)
      {
         log.info(text + ": messageID from messageReceived=" + message.getMessage().getMessageID() + " message = " + message + " content=" + txtMessage.getText());
      }
      else
      {
         log.info(text + ": Message received was null");
      }
      if (shouldAssert)
      {
         if (shouldBeNull)
         {
            assertNull(message);
         }
         else
         {
            assertNotNull(message);
         }
      }
   }

   // Inner classes -------------------------------------------------
   
}

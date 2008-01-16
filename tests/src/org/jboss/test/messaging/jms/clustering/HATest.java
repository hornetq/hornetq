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

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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
//   public void testAddToFailoverMapOnKill() throws Exception
//   {
//   	Connection conn = null;
//   	
//   	try
//   	{	   	
//	   	conn = this.createConnectionOnServer(cf, 0);
//	   	
//	   	Map failoverMap = ((ClientClusteredConnectionFactoryDelegate)(cf.getDelegate())).getFailoverMap();
//	   	
//	   	dumpFailoverMap(failoverMap);
//	   		   	
//	   	assertEquals(3, failoverMap.size());
//	   	
//	   	Map mapCopy = new HashMap(failoverMap);
//	   	
//	   	ServerManagement.kill(1);
//	   	
//	   	Thread.sleep(5000);
//	   		   	
//	   	failoverMap = ((ClientClusteredConnectionFactoryDelegate)(cf.getDelegate())).getFailoverMap();
//	   	
//	   	dumpFailoverMap(failoverMap);
//	   	
//	   	//Failover map should be added to not replaced
//	   	
//	   	assertEquals(3, failoverMap.size());
//	   	
//	   	assertEquals(2, ((Integer)failoverMap.get(new Integer(0))).intValue());
//	   	
//	   	assertEquals(0, ((Integer)failoverMap.get(new Integer(2))).intValue());
//	   	
//	   	assertEquals(((Integer)mapCopy.get(new Integer(1))).intValue(), ((Integer)failoverMap.get(new Integer(1))).intValue());
//   	}
//   	finally
//   	{
//   		if (conn != null)
//   		{
//   			conn.close();
//   		}
//   	}
//   }
//   
//   private void dumpFailoverMap(Map failoverMap)
//   {
//      log.info("Dumping failover map");
//      Iterator iter = failoverMap.entrySet().iterator();
//      while (iter.hasNext())
//      {
//      	Map.Entry entry = (Map.Entry)iter.next();
//      	log.info(entry.getKey() + "-->" + entry.getValue());
//      }
//   }
//   
//   
//
//   /**
//    * This test was created as per http://jira.jboss.org/jira/browse/JBMESSAGING-685.
//    */
//   public void testEmptyCommit() throws Exception
//   {
//      Connection conn = cf.createConnection();
//      JBossSession session = (JBossSession)conn.createSession(true, Session.SESSION_TRANSACTED);
//      session.commit();
//      conn.close();
//   }
//
//   /**
//    * This test was created as per http://jira.jboss.org/jira/browse/JBMESSAGING-696.
//    */
//   public void testCloseOnFailover() throws Exception
//   {
//      Connection conn1 = createConnectionOnServer(cf,0);
//      Connection conn2 = createConnectionOnServer(cf,1);
//      Connection conn3 = createConnectionOnServer(cf,2);
//      
//      SimpleFailoverListener failoverListener = new SimpleFailoverListener();
//      ((JBossConnection)conn2).registerFailoverListener(failoverListener);
//      
//      ServerManagement.kill(1);
//
//      //wait for the client-side failover to complete
//
//      while(true)
//      {
//         FailoverEvent event = failoverListener.getEvent(30000);
//         if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
//         {
//            break;
//         }
//         if (event == null)
//         {
//            fail("Did not get expected FAILOVER_COMPLETED event");
//         }
//      }
//
//      ConnectionState state2 = getConnectionState(conn2);
//      ConnectionState state3 = getConnectionState(conn3);
//
//      assertNotSame(state2.getRemotingConnection(), state3.getRemotingConnection());
//      assertNotSame(state2.getRemotingConnection().getRemotingClient(),
//                    state3.getRemotingConnection().getRemotingClient());
//
//      conn1.close();
//
//      assertNotNull(state2.getRemotingConnection());
//      assertNotNull(state2.getRemotingConnection().getRemotingClient());
//      assertTrue(state2.getRemotingConnection().getRemotingClient().isConnected());
//
//      conn2.close();
//
//      assertNotNull(state3.getRemotingConnection());
//      assertNotNull(state3.getRemotingConnection().getRemotingClient());
//      assertTrue(state3.getRemotingConnection().getRemotingClient().isConnected());
//
//      // When I created the testcase this was failing, throwing exceptions. This was basically why
//      // I created this testcase
//      conn3.close();
//   }
//
//   /*
//    * Test that connections created using a clustered connection factory are created round robin on
//    * different servers
//    */
//   public void testRoundRobinConnectionCreation() throws Exception
//   {
//      ClientClusteredConnectionFactoryDelegate delegate =
//         (ClientClusteredConnectionFactoryDelegate)cf.getDelegate();
//
//      assertEquals(3, delegate.getDelegates().length);
//
//      ClientConnectionFactoryDelegate cf1 = delegate.getDelegates()[0];
//
//      ClientConnectionFactoryDelegate cf2 = delegate.getDelegates()[1];
//
//      ClientConnectionFactoryDelegate cf3 = delegate.getDelegates()[2];
//
//      assertEquals(0, cf1.getServerID());
//
//      assertEquals(1, cf2.getServerID());
//
//      assertEquals(2, cf3.getServerID());
//
//      assertEquals(3, ServerManagement.getServer(0).getNodeIDView().size());
//
//      Connection conn1 = null;
//
//      Connection conn2 = null;
//
//      Connection conn3 = null;
//
//      Connection conn4 = null;
//
//      Connection conn5 = null;
//
//      try
//      {
//         conn1 = createConnectionOnServer(cf, 0);  //server 0
//
//         conn2 = createConnectionOnServer(cf, 1);  //server 1
//
//         conn3 = createConnectionOnServer(cf, 2);  //server 2
//
//         conn4 = createConnectionOnServer(cf, 0);  //server 0
//
//         conn5 = createConnectionOnServer(cf, 1);  //server 1
//
//         int serverID1 = getServerId(conn1);
//
//         int serverID2 = getServerId(conn2);
//
//         int serverID3 = getServerId(conn3);
//
//         int serverID4 = getServerId(conn4);
//
//         int serverID5 = getServerId(conn5);
//
//         assertEquals(0, serverID1);
//
//         assertEquals(1, serverID2);
//
//         assertEquals(2, serverID3);
//
//         assertEquals(0, serverID4);
//
//         assertEquals(1, serverID5);
//      }
//      finally
//      {
//         if (conn1 != null)
//         {
//            conn1.close();
//         }
//
//         if (conn2 != null)
//         {
//            conn2.close();
//         }
//
//         if (conn3 != null)
//         {
//            conn3.close();
//         }
//
//         if (conn4 != null)
//         {
//            conn4.close();
//         }
//
//         if (conn5 != null)
//         {
//            conn5.close();
//         }
//      }
//
//   }
//
//   public void testSimpleFailover() throws Exception
//   {
//      Set nodeIDView = ServerManagement.getServer(0).getNodeIDView();
//      assertEquals(3, nodeIDView.size());
//
//      Connection conn = null;
//
//      try
//      {
//         conn = createConnectionOnServer(cf, 1);
//         
//         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
//         ((JBossConnection)conn).registerFailoverListener(failoverListener);
//
//         assertEquals(1, getServerId(conn));
//
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         MessageProducer prod = sess.createProducer(queue[1]);
//
//         MessageConsumer cons = sess.createConsumer(queue[1]);
//
//         final int NUM_MESSAGES = 100;
//
//         for (int i = 0; i < NUM_MESSAGES; i++)
//         {
//            TextMessage tm = sess.createTextMessage("message:" + i);
//
//            prod.send(tm);
//         }
//
//         //So now, messages should be in queue[1] on server 1
//         //So we now kill server 1
//         //Which should cause transparent failover of connection conn onto server 1
//
//         ServerManagement.kill(1);
//
//         //wait for the client-side failover to complete
//
//         while(true)
//         {
//            FailoverEvent event = failoverListener.getEvent(30000);
//            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
//            {
//               break;
//            }
//            if (event == null)
//            {
//               fail("Did not get expected FAILOVER_COMPLETED event");
//            }
//         }
//
//         conn.start();
//
//         for (int i = 0; i < NUM_MESSAGES; i++)
//         {
//            TextMessage tm = (TextMessage)cons.receive(1000);
//
//            assertNotNull(tm);
//
//            assertEquals("message:" + i, tm.getText());
//         }
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//   }
//
//
//   public void testFailoverWithUnackedMessagesClientAcknowledge() throws Exception
//   {
//      Set nodeIDView = ServerManagement.getServer(0).getNodeIDView();
//      assertEquals(3, nodeIDView.size());
//
//      Connection conn = null;
//
//      try
//      {
//         conn = createConnectionOnServer(cf, 1);
//
//         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
//         ((JBossConnection)conn).registerFailoverListener(failoverListener);
//
//         ClientConnectionDelegate del = getDelegate(conn);
//
//         ConnectionState state = (ConnectionState)del.getState();
//
//         int initialServerID = state.getServerID();
//
//         assertEquals(1, initialServerID);
//
//         Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//
//         MessageProducer prod = sess.createProducer(queue[1]);
//
//         MessageConsumer cons = sess.createConsumer(queue[1]);
//
//         final int NUM_MESSAGES = 100;
//
//         for (int i = 0; i < NUM_MESSAGES; i++)
//         {
//            TextMessage tm = sess.createTextMessage("message:" + i);
//
//            prod.send(tm);
//         }
//
//         conn.start();
//
//         //Now consume half of the messages but don't ack them these will end up in
//         //client side toAck list
//
//         for (int i = 0; i < NUM_MESSAGES / 2; i++)
//         {
//            TextMessage tm = (TextMessage)cons.receive(1000);
//
//            assertNotNull(tm);
//
//            assertEquals("message:" + i, tm.getText());
//         }
//
//         //So now, messages should be in queue[1] on server 1
//         //So we now kill server 1
//         //Which should cause transparent failover of connection conn onto server 2
//
//         ServerManagement.kill(1);
//
//         // wait for the client-side failover to complete
//
//         while(true)
//         {
//            FailoverEvent event = failoverListener.getEvent(30000);
//            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
//            {
//               break;
//            }
//            if (event == null)
//            {
//               fail("Did not get expected FAILOVER_COMPLETED event");
//            }
//         }
//
//
//         state = (ConnectionState)del.getState();
//
//         int finalServerID = state.getServerID();
//
//         conn.start();
//
//         //Now should be able to consume the rest of the messages
//
//         TextMessage tm = null;
//
//         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
//         {
//            tm = (TextMessage)cons.receive(5000);
//            
//            assertNotNull(tm);
//            
//            assertEquals("message:" + i, tm.getText());
//         }
//
//         //Now should be able to acknowledge them
//
//         tm.acknowledge();
//
//         //Now check there are no more messages there
//         sess.close();
//
//         checkEmpty(queue[finalServerID], finalServerID);
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            try
//            {
//               conn.close();
//            }
//            catch (Exception e)
//            {
//               e.printStackTrace();
//            }
//         }
//      }
//
//   }
//
//     
//   public void testFailoverWithUnackedMessagesTransactional() throws Exception
//   {
//      Set nodeIDView = ServerManagement.getServer(0).getNodeIDView();
//      assertEquals(3, nodeIDView.size());
//     
//      Connection conn = null;
//
//      try
//      {
//         //Get a connection on server 1
//
//         conn = createConnectionOnServer(cf, 1);
//         
//         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
//         ((JBossConnection)conn).registerFailoverListener(failoverListener);
//
//         ClientConnectionDelegate del = getDelegate(conn);
//
//         ConnectionState state = (ConnectionState)del.getState();
//
//         int initialServerID = state.getServerID();
//
//         assertEquals(1, initialServerID);
//
//         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
//
//         MessageProducer prod = sess.createProducer(queue[1]);
//
//         MessageConsumer cons = sess.createConsumer(queue[1]);
//
//         final int NUM_MESSAGES = 100;
//
//         for (int i = 0; i < NUM_MESSAGES; i++)
//         {
//            TextMessage tm = sess.createTextMessage("message:" + i);
//
//            prod.send(tm);
//         }
//
//         sess.commit();
//
//         conn.start();
//
//         //Now consume half of the messages but don't commit them these will end up in
//         //client side resource manager
//
//         for (int i = 0; i < NUM_MESSAGES / 2; i++)
//         {
//            TextMessage tm = (TextMessage)cons.receive(2000);
//
//            assertNotNull(tm);
//
//            assertEquals("message:" + i, tm.getText());
//         }
//
//         //So now, messages should be in queue[1] on server 1
//         //So we now kill server 1
//         //Which should cause transparent failover of connection conn onto server 2
//         
//         ServerManagement.kill(1);
//
//         //       wait for the client-side failover to complete
//
//         while(true)
//         {
//            FailoverEvent event = failoverListener.getEvent(30000);
//            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
//            {
//               break;
//            }
//            if (event == null)
//            {
//               fail("Did not get expected FAILOVER_COMPLETED event");
//            }
//         }
//         state = (ConnectionState)del.getState();
//
//         int finalServerID = state.getServerID();
//
//         conn.start();
//
//         //Now should be able to consume the rest of the messages
//
//         TextMessage tm = null;
//
//         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
//         {
//            tm = (TextMessage)cons.receive(5000);
//
//            assertNotNull(tm);
//
//            assertEquals("message:" + i, tm.getText());
//         }
//
//         //Now should be able to commit them
//
//         sess.commit();
//
//         //Now check there are no more messages there
//         sess.close();
//
//         checkEmpty(queue[1], finalServerID);
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            try
//            {
//               conn.close();
//            }
//            catch (Exception e)
//            {
//               e.printStackTrace();
//            }
//         }
//      }
//   }
//
//   public void testTopicSubscriber() throws Exception
//   {
//      Destination destination = (Destination) topic[1];
//
//      JBossConnection conn = (JBossConnection)createConnectionOnServer(cf, 1);
//      
//      SimpleFailoverListener failoverListener = new SimpleFailoverListener();
//      ((JBossConnection)conn).registerFailoverListener(failoverListener);
//
//
//      conn.setClientID("testClient");
//      conn.start();
//
//      try
//      {
//         JBossSession session = (JBossSession) conn.createSession(true, Session.SESSION_TRANSACTED);
//         ClientSessionDelegate clientSessionDelegate = getDelegate(session);
//         SessionState sessionState = (SessionState) clientSessionDelegate.getState();
//
//         MessageConsumer consumerHA = session.createDurableSubscriber((Topic) destination, "T1");
//
//         MessageProducer producer = session.createProducer(destination);
//         Message message = session.createTextMessage("Hello Before");
//         producer.send(message);
//         session.commit();
//
//         receiveMessage("consumerHA", consumerHA, true, false);
//
//         session.commit();
//         //if (true) return;
//
//         Object txID = sessionState.getCurrentTxId();
//
//         producer.send(session.createTextMessage("Hello again before failover"));
//
//         ClientConnectionDelegate delegate = getDelegate(conn);
//
//         JMSRemotingConnection originalRemoting = delegate.getRemotingConnection();
//
//         ServerManagement.kill(1);
//
//         // wait for the client-side failover to complete
//
//         while(true)
//         {
//            FailoverEvent event = failoverListener.getEvent(30000);
//            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
//            {
//               break;
//            }
//            if (event == null)
//            {
//               fail("Did not get expected FAILOVER_COMPLETED event");
//            }
//         }
//         // if failover happened, this object was replaced
//         assertNotSame(originalRemoting, delegate.getRemotingConnection());
//
//         message = session.createTextMessage("Hello After");
//         producer.send(message);
//
//         assertEquals(txID, sessionState.getCurrentTxId());
// 
//         session.commit();
//
//         receiveMessage("consumerHA", consumerHA, true, false);
//         receiveMessage("consumerHA", consumerHA, true, false);
//         receiveMessage("consumerHA", consumerHA, true, true);
//
//         session.commit();
//
//         consumerHA.close();
//         session.unsubscribe("T1");
//      }
//      finally
//      {
//         if (conn!=null)
//         {
//            try { conn.close(); } catch (Throwable ignored) {}
//         }
//      }
//   }
//
//   /** testcase for http://jira.jboss.com/jira/browse/JBMESSAGING-1038 */
//   public void testHopping() throws Exception
//   {
//
//      JBossConnectionFactory localcf = (JBossConnectionFactory)ic[0].lookup("/ClusteredConnectionFactory");
//      ClientClusteredConnectionFactoryDelegate cfdelegate = (ClientClusteredConnectionFactoryDelegate)localcf.getDelegate();
//
//      ((ClientClusteredConnectionFactoryDelegate)cf.getDelegate()).closeCallback();
//
//      // After this, the CF won't get any callbacks
//      cfdelegate.closeCallback();
//
//      Connection conn = createConnectionOnServer(localcf, 1);
//
//      try
//      {
//
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageProducer producer = sess.createProducer(queue[1]);
//
//         MessageConsumer consumer = sess.createConsumer(queue[1]);
//
//         conn.start();
//
//         producer.send(sess.createTextMessage("sent before kill"));
//         TextMessage msg = (TextMessage)consumer.receive(2000);
//
//         assertNotNull(msg);
//         assertEquals("sent before kill", msg.getText());
//
//         ServerManagement.kill(2);
//         ServerManagement.kill(1);
//
//         // We need to guarantee we still have the old failover map (before the topology change for the test to be valid)
//         assertEquals(3, cfdelegate.getDelegates().length);
//
//         log.info("Sending Message");
//         producer.send(sess.createTextMessage("sent after kill"));
//         msg = (TextMessage) consumer.receive(2000);
//
//         assertNotNull(msg);
//         assertEquals("sent after kill", msg.getText());
//
//         assertEquals(0, getServerId(conn));
//      }
//      finally
//      {
//         conn.close();
//      }
//
//   }
//
//   // Package protected ---------------------------------------------
//   
//   // Protected -----------------------------------------------------
//   
//   protected void setUp() throws Exception
//   {
//      nodeCount = 3;
//
//      super.setUp();
//   }
//   
//   // Private -------------------------------------------------------
//
//   private void receiveMessage(String text, MessageConsumer consumer, boolean shouldAssert, boolean shouldBeNull) throws Exception
//   {
//      TextMessage message = (TextMessage) consumer.receive(3000);
//      if (message != null)
//      {
//         log.info(text + ": messageID from messageReceived=" + message.getJMSMessageID() + " message = " + message + " content=" + message.getText());
//      }
//      else
//      {
//         log.info(text + ": Message received was null");
//      }
//      if (shouldAssert)
//      {
//         if (shouldBeNull)
//         {
//            assertNull(message);
//         }
//         else
//         {
//            assertNotNull(message);
//         }
//      }
//   }

   // Inner classes -------------------------------------------------
   
}

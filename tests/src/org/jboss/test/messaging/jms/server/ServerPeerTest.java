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
package org.jboss.test.messaging.jms.server;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageStatistics;
import org.jboss.jms.tx.MessagingXid;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerPeerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public ServerPeerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("this test is not supposed to run in a remote configuration!");
      }

      super.setUp();
      ServerManagement.start("all");
      
      
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testNonContextAlreadyBound() throws Exception
   {
      ServerManagement.stopServerPeer();

      initialContext.bind("/some-new-context", new Object());

      try
      {
         ServerManagement.startServerPeer(0, "/some-new-context", null);
         fail("should throw exception");
      }
      catch(MessagingJMSException e)
      {
         // OK
      }
   }

   public void testChangeDefaultJNDIContexts() throws Exception
   {
      ServerManagement.stopServerPeer();
     
      ServerManagement.startServerPeer(0, "/here-go-queues", "/and-here-topics/etc/etc");
      
      try
      {
         ServerManagement.deployQueue("SomeQueue");
         ServerManagement.deployTopic("SomeTopic");


         Queue q = (Queue)initialContext.lookup("/here-go-queues/SomeQueue");
         Topic t = (Topic)initialContext.lookup("/and-here-topics/etc/etc/SomeTopic");

         assertEquals("SomeQueue", q.getQueueName());
         assertEquals("SomeTopic", t.getTopicName());

      }
      finally
      {
         ServerManagement.undeployQueue("SomeQueue");
         ServerManagement.undeployTopic("SomeTopic");
         ServerManagement.stopServerPeer();
      }
   }

   public void testUnbindContexts() throws Exception
   {

      if(!ServerManagement.isServerPeerStarted())
      {
         ServerManagement.startServerPeer();
      }

      Context c = (Context)initialContext.lookup("/queue");
      c = (Context)initialContext.lookup("/topic");

      log.trace("context: " + c);

      ServerManagement.stopServerPeer();

      try
      {
         c = (Context)initialContext.lookup("/queue");
         fail("this should fail");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      try
      {
         c = (Context)initialContext.lookup("/topic");
         fail("this should fail");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }

   //Full DLQ functionality is tested in DLQTest
   public void testSetGetDefaultDLQ() throws Exception
   {
      if(!ServerManagement.isServerPeerStarted())
      {
         ServerManagement.startServerPeer();
      }
            
      ObjectName on = new ObjectName("jboss.messaging.destination:service=Queue,name=DefaultDLQ");
      
      ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "DefaultDLQ", on.toString());
      
      ObjectName defaultDLQ =
         (ObjectName)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "DefaultDLQ");

      assertEquals(on, defaultDLQ);
   }
   
   //Full Expiry Queue functionality is tested in ExpiryQueueTest
   public void testSetGetDefaultExpiryQueue() throws Exception
   {
      if(!ServerManagement.isServerPeerStarted())
      {
         ServerManagement.startServerPeer();
      }
            
      ObjectName on = new ObjectName("jboss.messaging.destination:service=Queue,name=DefaultExpiry");
      
      ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "DefaultExpiryQueue", on.toString());
      
      ObjectName defaultExpiry =
         (ObjectName)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "DefaultExpiryQueue");

      assertEquals(on, defaultExpiry);
   }
   
   
   public void testRetrievePreparedTransactions() throws Exception
   {
      if(!ServerManagement.isServerPeerStarted())
      {
         ServerManagement.startServerPeer();
      }
      
      XAConnectionFactory cf = (XAConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.deployQueue("Queue");

      Queue queue = (Queue)initialContext.lookup("/queue/Queue");
      
      XAConnection conn = null;
      
      try
      {
         conn = cf.createXAConnection();
         
         Xid xid1, xid2;
         
         {
         
            XASession sess = conn.createXASession();
            
            XAResource res = sess.getXAResource();
            
            MessageProducer prod = sess.createProducer(queue);
            
            xid1 = new MessagingXid("blah1".getBytes(), 42, "blahblah1".getBytes());
                     
            TextMessage tm = sess.createTextMessage("message1");
            
            res.start(xid1, XAResource.TMNOFLAGS);
            
            prod.send(tm);
            
            res.end(xid1, XAResource.TMSUCCESS);
            
            res.prepare(xid1);
         
         }
         
         {
            
            XASession sess = conn.createXASession();
            
            XAResource res = sess.getXAResource();
            
            MessageProducer prod = sess.createProducer(queue);
            
            xid2 = new MessagingXid("blah2".getBytes(), 42, "blahblah2".getBytes());
                     
            TextMessage tm = sess.createTextMessage("message1");
            
            res.start(xid2, XAResource.TMNOFLAGS);
            
            prod.send(tm);
            
            res.end(xid2, XAResource.TMSUCCESS);
            
            res.prepare(xid2);
         
         }
         
         List txList = (List)ServerManagement.invoke(ServerManagement.getServerPeerObjectName(), 
                  "retrievePreparedTransactions", null, null);
         
         assertNotNull(txList);
         
         assertEquals(2, txList.size());
         
         Xid rxid1 = (Xid)txList.get(0);
         
         Xid rxid2 = (Xid)txList.get(1);
         
         
         boolean ok = (xid1.equals(rxid1) && xid2.equals(rxid2)) ||
                      (xid2.equals(rxid1) && xid1.equals(rxid2));
         
         assertTrue(ok);
         
         String listAsHTML = (String)ServerManagement.invoke(ServerManagement.getServerPeerObjectName(), 
                  "showPreparedTransactionsAsHTML", null, null);
         
         assertNotNull(listAsHTML); 
         
         assertTrue(listAsHTML.indexOf(xid1.toString()) != -1);
         
         assertTrue(listAsHTML.indexOf(xid2.toString()) != -1);                      
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         
         ServerManagement.undeployQueue("Queue");
      }
   }
   
   public void testMessageCounter() throws Exception
   {
      if(!ServerManagement.isServerPeerStarted())
      {
         ServerManagement.startServerPeer();
      }
      
      ServerManagement.invoke(ServerManagement.getServerPeerObjectName(), "enableMessageCounters", null, null);
      
      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.deployQueue("Queue1");
      
      ServerManagement.deployQueue("Queue2");
      
      ServerManagement.deployQueue("Queue3");
      
      ServerManagement.deployTopic("Topic1");
      
      ServerManagement.deployTopic("Topic2");
      
      Queue queue1 = (Queue)initialContext.lookup("/queue/Queue1");
      
      Queue queue2 = (Queue)initialContext.lookup("/queue/Queue2");
      
      Queue queue3 = (Queue)initialContext.lookup("/queue/Queue3");
      
      Topic topic1 = (Topic)initialContext.lookup("/topic/Topic1");
      
      Topic topic2 = (Topic)initialContext.lookup("/topic/Topic2");
      
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         conn.setClientID("wib");
         
         Integer i = (Integer)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "DefaultMessageCounterHistoryDayLimit");
         
         assertNotNull(i);
         
         assertEquals(-1, i.intValue());
         
         ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "DefaultMessageCounterHistoryDayLimit", String.valueOf(23));
         
         i = (Integer)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "DefaultMessageCounterHistoryDayLimit");
         
         assertNotNull(i);
         
         assertEquals(23, i.intValue());
         
         ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "DefaultMessageCounterHistoryDayLimit", String.valueOf(-100));
         
         i = (Integer)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "DefaultMessageCounterHistoryDayLimit");
         
         assertNotNull(i);
         
         assertEquals(-1, i.intValue());
         
         Long l = (Long)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "QueueStatsSamplePeriod");
         
         assertNotNull(l);
         
         assertEquals(5000, l.longValue()); //default value
         
         ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "QueueStatsSamplePeriod", String.valueOf(1000));
         
         l = (Long)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "QueueStatsSamplePeriod");
         
         assertNotNull(l);
         
         assertEquals(1000, l.longValue());
         
         
         List counters = (List)
            ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "MessageCounters");
         
         assertNotNull(counters);
         
         assertEquals(3, counters.size());
         
         
         //Create some subscriptions
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess.createConsumer(topic1);
         
         MessageConsumer cons2 = sess.createConsumer(topic2);
         
         MessageConsumer cons3 = sess.createDurableSubscriber(topic2, "sub1");
         
         
         counters = (List)
         ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "MessageCounters");
      
         assertNotNull(counters);
      
         //Should now be 6 - 3 more for subscriptions
         assertEquals(6, counters.size());
         
         
         Iterator iter = counters.iterator();
         
         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter)iter.next();
            
            assertEquals(0, counter.getCount());
            
            assertEquals(0, counter.getCountDelta());
            
            assertEquals(-1, counter.getHistoryLimit());
            
         }
         
         //Create a temp queue
         
         TemporaryQueue tempQueue = sess.createTemporaryQueue();
         
         counters = (List)
         ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "MessageCounters");
      
         assertNotNull(counters);
      
         //Should now be 7
         assertEquals(7, counters.size());
         
         //Send some messages
         
         MessageProducer prod = sess.createProducer(null);
         
         TextMessage tm1 = sess.createTextMessage("message1");
         
         TextMessage tm2 = sess.createTextMessage("message2");
         
         TextMessage tm3 = sess.createTextMessage("message3");
         
         prod.send(queue1, tm1);         
         prod.send(queue1, tm2);         
         prod.send(queue1, tm3);
         
         prod.send(queue2, tm1);         
         prod.send(queue2, tm2);         
         prod.send(queue2, tm3);
         
         prod.send(queue3, tm1);         
         prod.send(queue3, tm2);         
         prod.send(queue3, tm3);
         
         prod.send(tempQueue, tm1);         
         prod.send(tempQueue, tm2);         
         prod.send(tempQueue, tm3);
         
         prod.send(topic1, tm1);         
         prod.send(topic1, tm2);         
         prod.send(topic1, tm3);
         
         prod.send(topic2, tm1);         
         prod.send(topic2, tm2);         
         prod.send(topic2, tm3);
         
         iter = counters.iterator();
         
         //Wait until the stats are updated
         Thread.sleep(1500);
         
         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter)iter.next();
            
            assertEquals(3, counter.getCount());
            
            assertEquals(3, counter.getCountDelta());
            
            assertEquals(-1, counter.getHistoryLimit());
            
         }
         
         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter)iter.next();
                
            assertEquals(3, counter.getCount());
            
            assertEquals(0, counter.getCountDelta());
            
            assertEquals(-1, counter.getHistoryLimit());
            
         }
            
         ServerManagement.invoke(ServerManagement.getServerPeerObjectName(), "resetAllMessageCounters", null, null);
         
         ServerManagement.invoke(ServerManagement.getServerPeerObjectName(), "resetAllMessageCounterHistories", null, null);
         
         
         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter)iter.next();
            
            assertEquals(0, counter.getCount());
            
            assertEquals(0, counter.getCountDelta());
            
            assertEquals(-1, counter.getHistoryLimit());            
         }
         
         String html = (String)ServerManagement.invoke(ServerManagement.getServerPeerObjectName(), "listMessageCountersAsHTML", null, null);
         
         assertNotNull(html);
         
         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter)iter.next();

            assertTrue(html.indexOf(counter.getDestinationName()) != -1);           
         }
         
         List stats = (List)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "MessageStatistics");
         
         assertNotNull(stats);
         
         assertEquals(7, stats.size());
         
         iter = stats.iterator();
         
         while (iter.hasNext())
         {
            MessageStatistics stat = (MessageStatistics)iter.next();
            
            assertEquals(0, stat.getCount());
            
            assertEquals(0, stat.getCountDelta());
            
            assertEquals(3, stat.getDepth());
            
            assertEquals(0, stat.getDepthDelta());   
         }
         
         cons1.close();
         cons2.close();
         cons3.close();
         sess.unsubscribe("sub1");
         
         counters = (List)
         ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "MessageCounters");
      
         assertNotNull(counters);
      
         assertEquals(4, counters.size());
                  
         tempQueue.delete();
         
         
         counters = (List)
         ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "MessageCounters");
      
         assertNotNull(counters);
      
         assertEquals(3, counters.size());
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         
         ServerManagement.undeployQueue("Queue1");
         
         ServerManagement.undeployQueue("Queue2");
         
         ServerManagement.undeployQueue("Queue3");
         
         ServerManagement.undeployTopic("Topic1");
         
         ServerManagement.undeployTopic("Topic2");
         
         ServerManagement.invoke(ServerManagement.getServerPeerObjectName(), "disableMessageCounters", null, null);
      }
   }
   
   public void testGetDestinations() throws Exception
   {
      if (!ServerManagement.isServerPeerStarted())
      {
         ServerManagement.startServerPeer();
      }
              
      ServerManagement.deployQueue("Queue1");
      
      ServerManagement.deployQueue("Queue2");
      
      ServerManagement.deployQueue("Queue3");
      
      ServerManagement.deployTopic("Topic1");
      
      ServerManagement.deployTopic("Topic2");
      
      try
      {
      
         Set destinations =
            (Set)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "Destinations");
         
         assertNotNull(destinations);
         
         assertEquals(5, destinations.size());
      }
      finally
      {
         ServerManagement.undeployQueue("Queue1");
         
         ServerManagement.undeployQueue("Queue2");
         
         ServerManagement.undeployQueue("Queue3");
         
         ServerManagement.undeployTopic("Topic1");
         
         ServerManagement.undeployTopic("Topic2");
      }
         
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}

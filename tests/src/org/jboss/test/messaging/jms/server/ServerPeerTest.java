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

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageStatistics;
import org.jboss.jms.tx.MessagingXid;
import org.jboss.test.messaging.JBMServerTestCase;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public class ServerPeerTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final String HERE_GO_QUEUES = "/here-go-queues";
   private static final String AND_HERE_TOPICS_ETC_ETC = "/and-here-topics";
   private String origQueueContext;
   private String origTopicContext;

   // Constructors --------------------------------------------------

   public ServerPeerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------


   protected void setUp() throws Exception
   {
      if(servers.size()>0)
      {
         servers.get(0).stop();
      }
      super.setUp();
      origQueueContext = getJmsServer().getConfiguration().getDefaultQueueJNDIContext();
      origTopicContext = getJmsServer().getConfiguration().getDefaultTopicJNDIContext();
      getJmsServer().getConfiguration().setDefaultQueueJNDIContext(HERE_GO_QUEUES);
      getJmsServer().getConfiguration().setDefaultTopicJNDIContext(AND_HERE_TOPICS_ETC_ETC);
   }


   protected void tearDown() throws Exception
   {
      super.tearDown();
      getJmsServer().getConfiguration().setDefaultQueueJNDIContext(origQueueContext);
      getJmsServer().getConfiguration().setDefaultTopicJNDIContext(origTopicContext);
   }


   protected void deployAndLookupAdministeredObjects() throws Exception
   {
      getJmsServer().getConfiguration().setDefaultQueueJNDIContext(HERE_GO_QUEUES);
      getJmsServer().getConfiguration().setDefaultTopicJNDIContext(AND_HERE_TOPICS_ETC_ETC);
      deployTopic("Topic1");
      deployTopic("Topic2");
      deployTopic("Topic3");
      deployQueue("Queue1");
      deployQueue("Queue2");
      deployQueue("Queue3");
      deployQueue("Queue4");

      InitialContext ic = getInitialContext();
      topic1 = (Topic) ic.lookup(AND_HERE_TOPICS_ETC_ETC + "/Topic1");
      topic2 = (Topic) ic.lookup(AND_HERE_TOPICS_ETC_ETC + "/Topic2");
      topic3 = (Topic) ic.lookup(AND_HERE_TOPICS_ETC_ETC + "/Topic3");
      queue1 = (Queue) ic.lookup(HERE_GO_QUEUES + "/Queue1");
      queue2 = (Queue) ic.lookup(HERE_GO_QUEUES + "/Queue2");
      queue3 = (Queue) ic.lookup(HERE_GO_QUEUES + "/Queue3");
      queue4 = (Queue) ic.lookup(HERE_GO_QUEUES + "/Queue4");
   }

   public void testNonContextAlreadyBound() throws Exception
   {
      /*getInitialContext().bind("/some-new-context", new Object());

      try
      {
         ServerManagement.start(0, "/some-new-context", null);
         fail("should throw exception");
      }
      catch(MessagingJMSException e)
      {
         // OK
      }*/
   }

   public void testChangeDefaultJNDIContexts() throws Exception
   {

      try
      {
         deployQueue("SomeQueue");
         deployTopic("SomeTopic");


         Queue q = (Queue) getInitialContext().lookup(HERE_GO_QUEUES + "/SomeQueue");
         Topic t = (Topic) getInitialContext().lookup(AND_HERE_TOPICS_ETC_ETC + "/SomeTopic");

         assertEquals("SomeQueue", q.getQueueName());
         assertEquals("SomeTopic", t.getTopicName());

      }
      catch (Exception e)
      {
         e.printStackTrace();
         fail(e.getMessage());
      }
      finally
      {
         undeployQueue("SomeQueue");
         undeployTopic("SomeTopic");
      }
   }

   public void testUnbindContexts() throws Exception
   {

      try
      {
         getInitialContext().lookup("/queue");
         fail("this should fail");
      }
      catch (NameNotFoundException e)
      {
         // OK
      }

      try
      {
         getInitialContext().lookup("/topic");
         fail("this should fail");
      }
      catch (NameNotFoundException e)
      {
         // OK
      }
   }

   //Full DLQ functionality is tested in DLQTest
   public void testSetGetDefaultDLQ() throws Exception
   {
      String dlq = "DLQ";
      Queue q = (Queue) getInitialContext().lookup(HERE_GO_QUEUES + "/" + dlq);

      assertEquals(dlq, q.getQueueName());
   }

   //Full Expiry Queue functionality is tested in ExpiryQueueTest
   public void testSetGetDefaultExpiryQueue() throws Exception
   {
      String expq = "ExpiryQueue";
      Queue q = (Queue) getInitialContext().lookup(HERE_GO_QUEUES + "/" + expq);

      assertEquals(expq, q.getQueueName());
   }


   public void testRetrievePreparedTransactions() throws Exception
   {
      XAConnectionFactory cf = (XAConnectionFactory) getInitialContext().lookup("/ConnectionFactory");

      deployQueue("Queue");

      Queue queue = (Queue) getInitialContext().lookup(HERE_GO_QUEUES + "/" + "Queue");

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

         List txList = getJmsServer().retrievePreparedTransactions();

         assertNotNull(txList);

         assertEquals(2, txList.size());

         Xid rxid1 = (Xid) txList.get(0);

         Xid rxid2 = (Xid) txList.get(1);


         boolean ok = (xid1.equals(rxid1) && xid2.equals(rxid2)) ||
                 (xid2.equals(rxid1) && xid1.equals(rxid2));

         assertTrue(ok);

         String listAsHTML = null;//getJmsServerStatistics().showPreparedTransactionsAsHTML();

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

         undeployQueue("Queue");
      }
   }

   public void testMessageCounter() throws Exception
   {
      int currentCounters = ((ServerPeer)getJmsServer()).getMessageCounters().size();
      getJmsServer().enableMessageCounters();
      Session sess = null;
      ConnectionFactory cf = (ConnectionFactory) getInitialContext().lookup("/ConnectionFactory");


      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         conn.setClientID("wib");

         Integer i = ((ServerPeer)getJmsServer()).getConfiguration().getDefaultMessageCounterHistoryDayLimit();

         assertNotNull(i);

         assertEquals(-1, i.intValue());

         ((ServerPeer)getJmsServer()).getConfiguration().setDefaultMessageCounterHistoryDayLimit(23);

         i = ((ServerPeer)getJmsServer()).getConfiguration().getDefaultMessageCounterHistoryDayLimit();

         assertNotNull(i);

         assertEquals(23, i.intValue());

         ((ServerPeer)getJmsServer()).getConfiguration().setDefaultMessageCounterHistoryDayLimit(-100);

         i = ((ServerPeer)getJmsServer()).getConfiguration().getDefaultMessageCounterHistoryDayLimit();

         assertNotNull(i);

         assertEquals(-1, i.intValue());

         Long l = ((ServerPeer)getJmsServer()).getConfiguration().getMessageCounterSamplePeriod();

         assertNotNull(l);

         assertEquals(5000, l.longValue()); //default value

         ((ServerPeer)getJmsServer()).getConfiguration().setMessageCounterSamplePeriod(1000);

         l = ((ServerPeer)getJmsServer()).getConfiguration().getMessageCounterSamplePeriod();

         assertNotNull(l);

         assertEquals(1000, l.longValue());


         List counters = ((ServerPeer)getJmsServer()).getMessageCounters();

         assertNotNull(counters);

         //assertEquals(currentCounters + 3, counters.size());

         //Create some subscriptions

         sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons1 = sess.createConsumer(topic1);

         MessageConsumer cons2 = sess.createConsumer(topic2);

         MessageConsumer cons3 = sess.createDurableSubscriber(topic2, "sub1");


         counters = ((ServerPeer)getJmsServer()).getMessageCounters();

         assertNotNull(counters);

         //Should now be currentcounters + 3 more for subscriptions
         assertEquals(currentCounters + 3, counters.size());


         Iterator iter = counters.iterator();

         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter) iter.next();

            assertEquals(0, counter.getCount());

            assertEquals(0, counter.getCountDelta());

            assertEquals(-1, counter.getHistoryLimit());

         }

         //Create a temp queue

         TemporaryQueue tempQueue = sess.createTemporaryQueue();

         counters = ((ServerPeer)getJmsServer()).getMessageCounters();

         assertNotNull(counters);

         //Temp queues don't have counters
         assertEquals(currentCounters + 6, counters.size());

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
            MessageCounter counter = (MessageCounter) iter.next();

            assertEquals(3, counter.getCount());

            assertEquals(3, counter.getCountDelta());

            assertEquals(-1, counter.getHistoryLimit());

         }

         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter) iter.next();

            assertEquals(3, counter.getCount());

            assertEquals(0, counter.getCountDelta());

            assertEquals(-1, counter.getHistoryLimit());

         }

         getJmsServer().resetAllMessageCounters();
         getJmsServer().resetAllMessageCounterHistories();

         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter) iter.next();

            assertEquals(0, counter.getCount());

            assertEquals(0, counter.getCountDelta());

            assertEquals(-1, counter.getHistoryLimit());
         }

         String html = null;//getJmsServer().listMessageCountersAsHTML();

         assertNotNull(html);

         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter) iter.next();

            assertTrue(html.indexOf(counter.getDestinationName()) != -1);
         }

         List stats = ((ServerPeer)getJmsServer()).getMessageStatistics();

         assertNotNull(stats);

         assertEquals(6, stats.size());

         iter = stats.iterator();

         while (iter.hasNext())
         {
            MessageStatistics stat = (MessageStatistics) iter.next();

            assertEquals(0, stat.getCount());

            assertEquals(0, stat.getCountDelta());

            assertEquals(3, stat.getDepth());

            assertEquals(0, stat.getDepthDelta());
         }

         cons1.close();
         cons2.close();
         cons3.close();
         sess.unsubscribe("sub1");

         counters = ((ServerPeer)getJmsServer()).getMessageCounters();

         assertNotNull(counters);

         assertEquals(3, counters.size());

         tempQueue.delete();


         counters = ((ServerPeer)getJmsServer()).getMessageCounters();

         assertNotNull(counters);

         assertEquals(3, counters.size());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         try
         {
            sess.unsubscribe("sub1");
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         undeployQueue("Queue1");

         undeployQueue("Queue2");

         undeployQueue("Queue3");

         undeployTopic("Topic1");

         undeployTopic("Topic2");

         getJmsServer().disableMessageCounters();
      }
   }

   public void testGetDestinations() throws Exception
   {

         Set destinations = ((ServerPeer)getJmsServer()).getDestinations();

         assertNotNull(destinations);

         assertEquals(5, destinations.size());


   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

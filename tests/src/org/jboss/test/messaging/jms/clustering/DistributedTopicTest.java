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

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;

/**
 * 
 * A DistributedTopicTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class DistributedTopicTest extends ClusteringTestBase
{

   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DistributedTopicTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testClusteredTopicNonDurableNonPersistent() throws Exception
   {
      clusteredTopicNonDurable(false);
   }

   public void testClusteredTopicNonDurablePersistent() throws Exception
   {
      clusteredTopicNonDurable(true);
   }

   public void testClusteredTopicNonDurableWithSelectorsNonPersistent() throws Exception
   {
      clusteredTopicNonDurableWithSelectors(false);
   }

   public void testClusteredTopicNonDurableWithSelectorsPersistent() throws Exception
   {
      clusteredTopicNonDurableWithSelectors(true);
   }

   public void testClusteredTopicDurableNonPersistent() throws Exception
   {
      clusteredTopicDurable(false);
   }

   public void testClusteredTopicDurablePersistent() throws Exception
   {
      clusteredTopicDurable(true);
   }

   public void testClusteredTopicSharedDurableLocalConsumerNonPersistent() throws Exception
   {
      clusteredTopicSharedDurableLocalConsumer(false);
   }

   public void testClusteredTopicSharedDurableLocalConsumerPersistent() throws Exception
   {
      clusteredTopicSharedDurableLocalConsumer(true);
   }

   public void testClusteredTopicSharedDurableNoLocalSubNonPersistent() throws Exception
   {
      clusteredTopicSharedDurableNoLocalSub(false);
   }

   public void testClusteredTopicSharedDurableNoLocalSubPersistent() throws Exception
   {
      clusteredTopicSharedDurableNoLocalSub(true);
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

   /*
    * Create non durable subscriptions on all nodes of the cluster.
    * Ensure all messages are receive as appropriate
    */
   private void clusteredTopicNonDurable(boolean persistent) throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;
      try
      {
         //This will create 3 different connection on 3 different nodes, since
         //the cf is clustered
         conn0 = cf.createConnection();
         conn1 = cf.createConnection();
         conn2 = cf.createConnection();
         
         log.info("Created connections");
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons0 = sess0.createConsumer(topic[0]);
         MessageConsumer cons1 = sess1.createConsumer(topic[1]);
         MessageConsumer cons2 = sess2.createConsumer(topic[2]);
         MessageConsumer cons3 = sess0.createConsumer(topic[0]);
         MessageConsumer cons4 = sess1.createConsumer(topic[1]);

         conn0.start();
         conn1.start();
         conn2.start();

         // Send at node 0

         MessageProducer prod = sess0.createProducer(topic[0]);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message" + i);

            prod.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons0.receive(1000);

            assertNotNull(tm);
            
            log.info("Got message " + tm.getText());

            assertEquals("message" + i, tm.getText());
         }
         
         Message msg = cons0.receive(1000);
         
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);

            assertNotNull(tm);
            
            log.info("Got message " + tm.getText());

            assertEquals("message" + i, tm.getText());
         }
         
         msg = cons1.receive(1000);
         
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);

            assertNotNull(tm);
            
            log.info("Got message " + tm.getText());

            assertEquals("message" + i, tm.getText());
         }
         
         msg = cons2.receive(1000);
         
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);

            assertNotNull(tm);
            
            log.info("Got message " + tm.getText());

            assertEquals("message" + i, tm.getText());
         }
         
         msg = cons3.receive(1000);
         
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons4.receive(1000);

            assertNotNull(tm);
            
            log.info("Got message " + tm.getText());

            assertEquals("message" + i, tm.getText());
         }
         
         msg = cons4.receive(1000);
         
         assertNull(msg);
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   /*
    * Create non durable subscriptions on all nodes of the cluster.
    * Include some with selectors
    * Ensure all messages are receive as appropriate
    */
   private void clusteredTopicNonDurableWithSelectors(boolean persistent) throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;

      try
      {
         //This will create 3 different connection on 3 different nodes, since
         //the cf is clustered
         conn0 = cf.createConnection();
         conn1 = cf.createConnection();
         conn2 = cf.createConnection();
         
         log.info("Created connections");
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons0 = sess0.createConsumer(topic[0]);
         MessageConsumer cons1 = sess1.createConsumer(topic[1]);
         MessageConsumer cons2 = sess2.createConsumer(topic[2]);
         MessageConsumer cons3 = sess0.createConsumer(topic[0], "COLOUR='red'");
         MessageConsumer cons4 = sess1.createConsumer(topic[1], "COLOUR='blue'");

         conn0.start();
         conn1.start();
         conn2.start();

         // Send at node 0

         MessageProducer prod = sess0.createProducer(topic[0]);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message" + i);

            int c = i % 3;
            if (c == 0)
            {
               tm.setStringProperty("COLOUR", "red");
            }
            else if (c == 1)
            {
               tm.setStringProperty("COLOUR", "blue");
            }

            prod.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons0.receive(1000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         Message msg = cons0.receive(1000);
         
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         msg = cons1.receive(1000);
         
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         msg = cons2.receive(1000);
         
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            int c = i % 3;

            if (c == 0)
            {
               TextMessage tm = (TextMessage)cons3.receive(1000);

               assertNotNull(tm);

               assertEquals("message" + i, tm.getText());
            }
         }
         
         msg = cons3.receive(1000);
         
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            int c = i % 3;

            if (c == 1)
            {
               TextMessage tm = (TextMessage)cons4.receive(1000);

               assertNotNull(tm);

               assertEquals("message" + i, tm.getText());
            }
         }
         
         msg = cons4.receive(1000);
         
         assertNull(msg);
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }

   private void clusteredTopicDurable(boolean persistent) throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;

      try
      {
         // This will create 3 different connection on 3 different nodes, since the cf is clustered
         conn0 = cf.createConnection();
         conn1 = cf.createConnection();
         conn2 = cf.createConnection();
         
         log.info("Created connections");
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1, conn2});

         conn0.setClientID("wib1");
         conn1.setClientID("wib1");
         conn2.setClientID("wib1");

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            sess0.unsubscribe("alpha");
         }
         catch (Exception ignore) {}
         try
         {
            sess1.unsubscribe("beta");
         }
         catch (Exception ignore) {}
         try
         {
            sess2.unsubscribe("gamma");
         }
         catch (Exception ignore) {}
         try
         {
            sess0.unsubscribe("delta");
         }
         catch (Exception ignore) {}
         try
         {
            sess1.unsubscribe("epsilon");
         }
         catch (Exception ignore) {}
         
         log.info("creating subs");

         MessageConsumer alpha = sess0.createDurableSubscriber(topic[0], "alpha");
         
         log.info("created 0");
         
         MessageConsumer beta = sess1.createDurableSubscriber(topic[1], "beta");
         
         log.info("created 1");
         
         MessageConsumer gamma = sess2.createDurableSubscriber(topic[2], "gamma");
         
         log.info("created 2");
         MessageConsumer delta = sess0.createDurableSubscriber(topic[0], "delta");
         
         log.info("created 3");
         
         MessageConsumer epsilon = sess1.createDurableSubscriber(topic[1], "epsilon");
         
         log.info("created 4");
         
         conn0.start();
         conn1.start();
         conn2.start();
         
         log.info("started");

         // Send at node 0 - and make sure the messages are consumable from all the durable subs

         MessageProducer prod = sess0.createProducer(topic[0]);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         log.info("sending messages");

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            prod.send(sess0.createTextMessage("message" + i));
         }
         
         log.info("Sent messages");

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)alpha.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            assertEquals("message" + i, tm.getText());
         }
         
         log.info("got 1");
         
         Message msg = alpha.receive(1000);
         assertNull(msg);         

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)beta.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            assertEquals("message" + i, tm.getText());
         }
         
         log.info("got 2");
         
         msg = beta.receive(1000);
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)gamma.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            assertEquals("message" + i, tm.getText());
         }
         
         log.info("got 3");
         
         msg = gamma.receive(1000);
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)delta.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            assertEquals("message" + i, tm.getText());
         }

         log.info("got 4");
         
         msg = delta.receive(1000);
         assertNull(msg);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)epsilon.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            assertEquals("message" + i, tm.getText());
         }
         
         
         log.info("got 5");
         
         msg = epsilon.receive(1000);
         assertNull(msg);
         
         
         //close beta
         beta.close();
         
         
         log.info("Sent messages");
         
         // Create another beta - this one node 0
         MessageConsumer beta0 = sess0.createDurableSubscriber(topic[0], "beta");
         
         //And one node node1
         MessageConsumer beta1 = sess1.createDurableSubscriber(topic[1], "beta");
         
         //Now send some more messages at node 2
         
         MessageProducer prod2 = sess2.createProducer(topic[2]);

         prod2.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         log.info("sending more messages");

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            prod2.send(sess1.createTextMessage("message2-" + i));
         }
         
         //They should be round - robined - but we don't know which one will get them first

         int offset = 0;
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)beta0.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            
            if (tm.getText().substring("message2-".length()).equals("1"))
            {
            	offset = 1;
            }
            
            assertEquals("message2-" + (i * 2 + offset), tm.getText());
         }
         
         msg = beta0.receive(2000);
         assertNull(msg);
         
         if (offset == 1)
         {
         	offset = 0;
         }
         else
         {
         	offset = 1;
         }      
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)beta1.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            assertEquals("message2-" + (i * 2 + offset), tm.getText());
         }
         
         msg = beta1.receive(2000);
         assertNull(msg);
         
         //Send some more at node 0
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            prod.send(sess1.createTextMessage("message3-" + i));
         }
         
         //This should go straight to the local queue
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)beta0.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            assertEquals("message3-" + i, tm.getText());
         }
         
         msg = beta0.receive(2000);
         assertNull(msg);
         
         //So now we have a beta on node 1 and a beta on node 0 and the messages are on node2
         
         beta0.close();
         beta1.close();	
         
         alpha.close();
         beta.close();
         gamma.close();
         delta.close();
         epsilon.close();
         
         log.info("got 6");

         sess0.unsubscribe("alpha");
         sess1.unsubscribe("beta");
         sess2.unsubscribe("gamma");
         sess0.unsubscribe("delta");
         sess1.unsubscribe("epsilon");
         
         log.info("got 7");

      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }
      }
   }
   
   

   /*
    * Create shared durable subs on multiple nodes, the local instance should always get the message
    */
   private void clusteredTopicSharedDurableLocalConsumer(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      Connection conn2 = null;
      Connection conn3 = null;
      try

      {
         //This will create 3 different connection on 3 different nodes, since
         //the cf is clustered
         conn1 = cf.createConnection();
         conn2 = cf.createConnection();
         conn3 = cf.createConnection();
         
         log.info("Created connections");
         
         checkConnectionsDifferentServers(new Connection[] {conn1, conn2, conn3});
         conn1.setClientID("wib1");
         conn2.setClientID("wib1");
         conn3.setClientID("wib1");

         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            sess1.unsubscribe("sub");
         }
         catch (Exception ignore) {}
         try
         {
            sess2.unsubscribe("sub");
         }
         catch (Exception ignore) {}
         try
         {
            sess3.unsubscribe("sub");
         }
         catch (Exception ignore) {}

         log.info("** creating 1");
         MessageConsumer cons1 = sess1.createDurableSubscriber(topic[0], "sub");
         log.info("** creating 2");
         MessageConsumer cons2 = sess2.createDurableSubscriber(topic[1], "sub");
         log.info("** creating 3");
         MessageConsumer cons3 = sess3.createDurableSubscriber(topic[2], "sub");
         log.info("** creating 4");

         conn1.start();
         conn2.start();
         conn3.start();

         // Send at node 0

         MessageProducer prod = sess1.createProducer(topic[0]);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message" + i);

            prod.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }

         Message m = cons2.receive(2000);

         assertNull(m);

         m = cons3.receive(2000);

         assertNull(m);

         // Send at node 1

         MessageProducer prod1 = sess2.createProducer(topic[1]);

         prod1.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess3.createTextMessage("message" + i);

            prod1.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }

         m = cons1.receive(2000);

         assertNull(m);

         m = cons3.receive(2000);

         assertNull(m);

         // Send at node 2

         MessageProducer prod2 = sess3.createProducer(topic[2]);

         prod2.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess3.createTextMessage("message" + i);

            prod2.send(tm);
         }

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons3.receive(1000);

            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }

         m = cons1.receive(2000);

         assertNull(m);

         m = cons2.receive(2000);

         assertNull(m);

         cons1.close();
         cons2.close();
         
         //Try and unsubscribe now - this should fail since there is still a consumer open on another node
         
         try
         {
         	sess1.unsubscribe("sub");
         	
         	fail("Did not throw IllegalStateException");
         }
         catch (javax.jms.IllegalStateException e)
         {
         	//Ok 
         }
         
         cons3.close();
         
         // Need to unsubscribe on any node that the durable sub was created on

         sess1.unsubscribe("sub");
         
         //Next unsubscribe should fail since it's already unsubscribed from a different node of the cluster
         try
         {
         	sess2.unsubscribe("sub");
         	
         	fail("Did not throw InvalidDestinationException");
         }
         catch (InvalidDestinationException e)
         {
         	//Ok
         }
         
         try
         {
         	sess3.unsubscribe("sub");
         	
         	fail("Did not throw InvalidDestinationException");
         }
         catch (InvalidDestinationException e)
         {
         	//Ok
         }
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
      }
   }



   /*
    * Create shared durable subs on multiple nodes, but without sub on local node
    * should round robin
    * note that this test assumes round robin
    */
   private void clusteredTopicSharedDurableNoLocalSub(boolean persistent) throws Exception
   {
      Connection conn1 = null;
      Connection conn2 = null;
      Connection conn3 = null;

      try
      {
         //This will create 3 different connection on 3 different nodes, since
         //the cf is clustered
         conn1 = cf.createConnection();
         conn2 = cf.createConnection();
         conn3 = cf.createConnection();
         
         log.info("Created connections");
         
         checkConnectionsDifferentServers(new Connection[] {conn1, conn2, conn3});
         
         conn2.setClientID("wib1");
         conn3.setClientID("wib1");

         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            sess2.unsubscribe("sub");
         }
         catch (Exception ignore) {}

         MessageConsumer cons1 = sess2.createDurableSubscriber(topic[1], "sub");
         MessageConsumer cons2 = sess3.createDurableSubscriber(topic[2], "sub");

         conn2.start();
         conn3.start();

         // Send at node 0

         //Should round robin between the other 2 since there is no active consumer on sub  on node 0

         MessageProducer prod = sess1.createProducer(topic[0]);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         final int NUM_MESSAGES = 100;

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("message2-" + i);

            prod.send(tm);
         }
         
         
         int offset = 0;
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            
            if (tm.getText().substring("message2-".length()).equals("1"))
            {
            	offset = 1;
            }
            
            assertEquals("message2-" + (i * 2 + offset), tm.getText());
         }
         
         Message msg = cons1.receive(2000);
         assertNull(msg);
         
         if (offset == 1)
         {
         	offset = 0;
         }
         else
         {
         	offset = 1;
         }      
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = (TextMessage)cons2.receive(1000);
            assertNotNull(tm);
            log.info("**** got message" + tm.getText());
            assertEquals("message2-" + (i * 2 + offset), tm.getText());
         }
         
         msg = cons2.receive(2000);
         assertNull(msg);
                 
         cons1.close();
         cons2.close();

         sess2.unsubscribe("sub");
         
         try
         {
         	sess3.unsubscribe("sub");
         	fail("Should already be unsubscribed");
         }
         catch (InvalidDestinationException e)
         {
         	//Ok - the previous unsubscribe should do a cluster wide unsubscribe
         }

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
      }
   }

  
   // Inner classes -------------------------------------------------
   

}

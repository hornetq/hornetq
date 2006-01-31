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
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.Topic;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.jms.Message;
import javax.jms.InvalidDestinationException;
import javax.management.ObjectName;
import java.util.Set;


/**
 * Tests focused on durable subscription behavior. More durable subscription tests can be found in
 * MessageConsumerTest.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class DurableSubscriptionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext ic;

   // Constructors --------------------------------------------------

   public DurableSubscriptionTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      log.debug("starting tear down");

      ic.close();

      ServerManagement.undeployTopic("Topic");

      super.tearDown();
   }

   public void testSimplestDurableSubscription() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      Topic topic = (Topic)ic.lookup("/topic/Topic");
      Connection conn = cf.createConnection();

      conn.setClientID("brookeburke");

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      s.createDurableSubscriber(topic, "monicabelucci");

      prod.send(s.createTextMessage("k"));

      conn.close();

      conn = cf.createConnection();
      conn.setClientID("brookeburke");

      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer durable = s.createDurableSubscriber(topic, "monicabelucci");

      conn.start();

      TextMessage tm = (TextMessage)durable.receive();
      assertEquals("k", tm.getText());


      Message m = durable.receive(1000);
      assertNull(m);
   }

   /**
    * JMS 1.1 6.11.1: A client can change an existing durable subscription by creating a durable
    * TopicSubscriber with the same name and a new topic and/or message selector, or NoLocal
    * attribute. Changing a durable subscription is equivalent to deleting and recreating it.
    *
    * Test with a different topic (a redeployed topic is a different topic).
    */
   public void testDurableSubscriptionOnTopicRedeployment() throws Exception
   {
      try
      {
         ic.lookup("/topic/CompletelyNewTopic");
         fail("should throw exception, topic shouldn't be deployed on the server");
      }
      catch(NamingException e)
      {
         // OK
      }

      ServerManagement.deployTopic("CompletelyNewTopic");

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      Topic topic = (Topic)ic.lookup("/topic/CompletelyNewTopic");
      Connection conn = cf.createConnection();

      conn.setClientID("brookeburke");

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      s.createDurableSubscriber(topic, "monicabelucci");

      prod.send(s.createTextMessage("one"));

      conn.close();

      ServerManagement.undeployTopic("CompletelyNewTopic");

      try
      {
         topic = (Topic)ic.lookup("/topic/CompletelyNewTopic");
         fail("should throw exception");
      }
      catch(NamingException e)
      {
         // OK
      }

      ServerManagement.deployTopic("CompletelyNewTopic");

      topic = (Topic)ic.lookup("/topic/CompletelyNewTopic");
      conn = cf.createConnection();

      conn.setClientID("brookeburke");

      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer durable = s.createDurableSubscriber(topic, "monicabelucci");

      conn.start();

      Message m = durable.receive(1000);
      assertNull(m);

      ServerManagement.undeployTopic("CompletelyNewTopic");
   }

   /**
    * JMS 1.1 6.11.1: A client can change an existing durable subscription by creating a durable
    * TopicSubscriber with the same name and a new topic and/or message selector, or NoLocal
    * attribute. Changing a durable subscription is equivalent to deleting and recreating it.
    *
    * Test with a different selector.
    */
   public void testDurableSubscriptionDifferentSelector() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      Topic topic = (Topic)ic.lookup("/topic/Topic");
      Connection conn = cf.createConnection();

      conn.setClientID("brookeburke");

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      MessageConsumer durable =
         s.createDurableSubscriber(topic,
                                   "monicabelucci",
                                   "color = 'red' AND shape = 'square'",
                                   false);

      TextMessage tm = s.createTextMessage("A red square message");
      tm.setStringProperty("color", "red");
      tm.setStringProperty("shape", "square");
      prod.send(tm);

      conn.start();

      TextMessage rm = (TextMessage)durable.receive(5000);
      assertEquals("A red square message", rm.getText());

      tm = s.createTextMessage("Another red square message");
      tm.setStringProperty("color", "red");
      tm.setStringProperty("shape", "square");
      prod.send(tm);

      // TODO: when subscriptions/durable subscription will be registered as MBean, use the JMX
      //       interface to make sure the 'another red square message' is maintained by the
      //       durable subascription
      //       http://jira.jboss.org/jira/browse/JBMESSAGING-217

      conn.close();

      conn = cf.createConnection();

      conn.setClientID("brookeburke");

      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // modify the selector
      durable = s.createDurableSubscriber(topic,
                                          "monicabelucci",
                                          "color = 'red'",
                                          false);

      conn.start();

      Message m = durable.receive(1000);

      // the durable subscription is destroyed and re-created. The red square message stored by
      // the previous durable subscription is lost and (hopefully) garbage collected.
      assertNull(m);
   }

   /**
    * JMS 1.1 6.11.1: A client can change an existing durable subscription by creating a durable
    * TopicSubscriber with the same name and a new topic and/or message selector, or NoLocal
    * attribute. Changing a durable subscription is equivalent to deleting and recreating it.
    *
    * Test with a different noLocal flag.
    */
   public void testDurableSubscriptionDifferentNoLocal() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      Topic topic = (Topic)ic.lookup("/topic/Topic");

      Connection conn = cf.createConnection();
      conn.setClientID("ID0");

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      MessageConsumer durable = s.createDurableSubscriber(topic, "mySubscription", null, false);

      TextMessage tm = null;
      tm = s.createTextMessage("Message One");
      prod.send(tm);
      tm = s.createTextMessage("Message Two");
      prod.send(tm);


      conn.start();

      TextMessage rm = (TextMessage)durable.receive(5000);
      assertEquals("Message One", rm.getText());

      // TODO: when subscriptions/durable subscription will be registered as MBean, use the JMX
      //       interface to make sure the 'another red square message' is maintained by the
      //       durable subascription

      conn.close();

      conn = cf.createConnection();
      conn.setClientID("ID0");
      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // modify the nolocal flag
      durable = s.createDurableSubscriber(topic, "mySubscription", null, true);

      conn.start();

      Message m = durable.receive(1000);

      // the durable subscription is destroyed and re-created. "Message Two" stored by the previous
      // durable subscription is lost and (hopefully) garbage collected.
      assertNull(m);
   }

   public void testDurableSubscriptionOnTemporaryTopic() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      Connection conn = cf.createConnection();
      conn.setClientID("doesn't actually matter");
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic temporaryTopic = s.createTemporaryTopic();

      try
      {
         s.createDurableSubscriber(temporaryTopic, "mySubscription");
         fail("this should throw exception");
      }
      catch(InvalidDestinationException e)
      {
         // OK
      }
   }

   public void testDurableSubscriptionCleanup() throws Exception
   {
      try
      {
         ic.lookup("/topic/AnotherCompletelyNewTopic");
         fail("should throw exception, topic shouldn't be deployed on the server");
      }
      catch(NamingException e)
      {
         // OK
      }

      ServerManagement.deployTopic("AnotherCompletelyNewTopic");

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      Topic topic = (Topic)ic.lookup("/topic/AnotherCompletelyNewTopic");
      Connection conn = cf.createConnection();

      conn.setClientID("blahblah");

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      s.createDurableSubscriber(topic, "ak47");

      conn.close();

      ObjectName dsson = ServerManagement.getDurableSubscriptionStoreObjectName();

      Set dsnames = (Set)ServerManagement.invoke(dsson, "listSubscriptions",
                                                 new Object[] { "blahblah" },
                                                 new String[] { "java.lang.String}"});

      assertEquals(1, dsnames.size());
      assertEquals("ak47", dsnames.iterator().next());

      ServerManagement.undeployTopic("AnotherCompletelyNewTopic");

      // the durable subscription should be cleared from memory

      dsson = ServerManagement.getDurableSubscriptionStoreObjectName();

      dsnames = (Set)ServerManagement.invoke(dsson, "listSubscriptions",
                                             new Object[] { "blahblah" },
                                             new String[] { "java.lang.String}"});

      assertTrue(dsnames.isEmpty());
   }

}

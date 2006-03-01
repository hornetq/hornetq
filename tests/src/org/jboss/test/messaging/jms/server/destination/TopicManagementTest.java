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
package org.jboss.test.messaging.jms.server.destination;

import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.jms.server.destination.base.DestinationManagementTestBase;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * Tests a topic's management interface.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TopicManagementTest extends DestinationManagementTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicManagementTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   /**
    * Test subscriptionCount() and subscriptionCount(boolean durable).
    * @throws Exception
    */
   public void testSubscriptionCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      TopicConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployTopic("TopicSubscription");
      Topic topic = (Topic)ic.lookup("/topic/TopicSubscription");

      TopicConnection conn = cf.createTopicConnection();

      conn.setClientID("brookeburke");

      TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      // Create 1 durable subscription and 2 non-durable subscription
      s.createDurableSubscriber(topic, "monicabelucci");
      
      s.createSubscriber(topic);
      s.createSubscriber(topic);

      // There should be 3 subscriptions
      ObjectName destObjectName = 
         new ObjectName("jboss.messaging.destination:service=Topic,name=TopicSubscription");
      Integer count = (Integer)ServerManagement.invoke(destObjectName, "subscriptionCount", null, null);
      assertEquals(3, count.intValue());
      
      // There should be 1 durable subscription
      Integer duraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(1, duraCount.intValue());
      
      // There should be 2 non-durable subscription
      Integer nonduraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(2, nonduraCount.intValue());
      
      // Now disconnect
      conn.close();
      
      // There should be only 1 subscription totally
      count = (Integer)ServerManagement.invoke(destObjectName, "subscriptionCount", null, null);
      assertEquals(1, count.intValue());
      
      // There should be 1 durable subscription
      duraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(1, duraCount.intValue());
      
      // There should be 0 non-durable subscription
      nonduraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(0, nonduraCount.intValue());
      
      // Now connect again and restore the durable subscription
      conn = cf.createTopicConnection();
      conn.setClientID("brookeburke");
      s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      s.createDurableSubscriber(topic, "monicabelucci");
      
      // There should be still 1 subscription
      count = (Integer)ServerManagement.invoke(destObjectName, "subscriptionCount", null, null);
      assertEquals(1, count.intValue());

      // Now create another durable subscription
      s.createDurableSubscriber(topic, "monicabelucci2");
      
      // There should be 2 durable subscription
      duraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(2, duraCount.intValue());
      
      // There should be 0 non-durable subscription
      nonduraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(0, nonduraCount.intValue());

      // And close the connection
      conn.close();
      
      // There should be 2 subscription still
      count = (Integer)ServerManagement.invoke(destObjectName, "subscriptionCount", null, null);
      assertEquals(2, count.intValue());

      // There should be 2 durable subscription
      duraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.TRUE}, 
            new String[] {"boolean"});
      assertEquals(2, duraCount.intValue());
      
      // There should be 0 non-durable subscription
      nonduraCount = (Integer)ServerManagement.invoke(
            destObjectName, 
            "subscriptionCount", 
            new Object[] {Boolean.FALSE}, 
            new String[] {"boolean"});
      assertEquals(0, nonduraCount.intValue());
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return false;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

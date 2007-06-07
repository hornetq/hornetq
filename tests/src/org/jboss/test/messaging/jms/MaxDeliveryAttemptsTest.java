/*
 * JBoss, Home of Professional Open Source
 * Copyright 2007, JBoss Inc., and individual contributors as indicated
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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A test for MaxDeliveryAttempts destination setting.
 *
 * @author <a href="sergey.koshcheyev@jboss.com">Sergey Koshcheyev</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class MaxDeliveryAttemptsTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
   private final String DLQ_NAME = "DLQ";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext ic;
   protected ConnectionFactory cf;
   protected Queue dlq;
   protected int defaultMaxDeliveryAttempts;

   // Constructors --------------------------------------------------
   
   public MaxDeliveryAttemptsTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testOverrideDefaultMaxDeliveryAttemptsForQueue() throws Exception
   {
      final String QUEUE_NAME = "Queue";
      
      ServerManagement.deployQueue(QUEUE_NAME);

      try
      {
         int maxDeliveryAttempts = defaultMaxDeliveryAttempts + 5;
         setMaxDeliveryAttempts(
               new ObjectName("jboss.messaging.destination:service=Queue,name=" + QUEUE_NAME),
               maxDeliveryAttempts);
         testMaxDeliveryAttempts("/queue/" + QUEUE_NAME, maxDeliveryAttempts);
      }
      finally
      {
         ServerManagement.undeployQueue(QUEUE_NAME);
      }
   }

   public void testOverrideDefaultMaxDeliveryAttemptsForTopic() throws Exception
   {
      final String TOPIC_NAME = "Topic";
      
      ServerManagement.deployTopic(TOPIC_NAME);

      try
      {
         int maxDeliveryAttempts = defaultMaxDeliveryAttempts + 5;
         setMaxDeliveryAttempts(
               new ObjectName("jboss.messaging.destination:service=Topic,name=" + TOPIC_NAME),
               maxDeliveryAttempts);

         testMaxDeliveryAttempts("/topic/" + TOPIC_NAME, maxDeliveryAttempts);
      }
      finally
      {
         ServerManagement.undeployTopic(TOPIC_NAME);
      }
   }
      
   public void testUseDefaultMaxDeliveryAttemptsForQueue() throws Exception
   {
      final String QUEUE_NAME = "Queue";
      
      ServerManagement.deployQueue(QUEUE_NAME);

      try
      {
         setMaxDeliveryAttempts(
               new ObjectName("jboss.messaging.destination:service=Queue,name=" + QUEUE_NAME),
               -1);

         // Check that defaultMaxDeliveryAttempts takes effect
         testMaxDeliveryAttempts("/queue/" + QUEUE_NAME, defaultMaxDeliveryAttempts);
      }
      finally
      {
         ServerManagement.undeployQueue(QUEUE_NAME);
      }
   }

   public void testUseDefaultMaxDeliveryAttemptsForTopic() throws Exception
   {
      final String TOPIC_NAME = "Topic";
      
      ServerManagement.deployTopic(TOPIC_NAME);

      try
      {
         setMaxDeliveryAttempts(
               new ObjectName("jboss.messaging.destination:service=Topic,name=" + TOPIC_NAME),
               -1);

         // Check that defaultMaxDeliveryAttempts takes effect
         testMaxDeliveryAttempts("/topic/" + TOPIC_NAME, defaultMaxDeliveryAttempts);
      }
      finally
      {
         ServerManagement.undeployTopic(TOPIC_NAME);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void setMaxDeliveryAttempts(ObjectName dest, int maxDeliveryAttempts) throws Exception
   {
      ServerManagement.setAttribute(dest, "MaxDeliveryAttempts",
            Integer.toString(maxDeliveryAttempts));
   }
   
   protected void testMaxDeliveryAttempts(String destJndiName, int destMaxDeliveryAttempts) throws Exception
   {
      Destination destination = (Destination) ic.lookup(destJndiName);
      
      Connection conn = cf.createConnection();
      
      try
      {
         // Create the consumer before the producer so that the message we send doesn't
         // get lost if the destination is a Topic.
         Session consumingSession = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);         
         MessageConsumer destinationConsumer = consumingSession.createConsumer(destination);
         
         {
            Session producingSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer prod = producingSession.createProducer(destination);
            TextMessage tm = producingSession.createTextMessage("Message");
            prod.send(tm);
         }

         conn.start();

         // Make delivery attempts up to the maximum. The message should not end up in the DLQ.
         for (int i = 0; i < destMaxDeliveryAttempts; i++)
         {
            TextMessage tm = (TextMessage)destinationConsumer.receive(1000);
            assertNotNull("No message received on delivery attempt number " + (i + 1), tm);
            assertEquals("Message", tm.getText());
            consumingSession.recover();
         }

         // At this point the message should not yet be in the DLQ
         MessageConsumer dlqConsumer = consumingSession.createConsumer(dlq);
         Message m = dlqConsumer.receive(1000);
         assertNull(m);
         
         // Now we try to consume the message again from the destination, which causes it
         // to go to the DLQ instead.
         m = destinationConsumer.receive(1000);
         assertNull(m);
         
         // The message should be in the DLQ now
         m = dlqConsumer.receive(1000);
         assertNotNull(m);
         assertTrue(m instanceof TextMessage);
         assertEquals("Message", ((TextMessage) m).getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");
      ServerManagement.deployQueue(DLQ_NAME);

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      dlq = (Queue) ic.lookup("/queue/" + DLQ_NAME);         

      drainDestination(cf, dlq);

      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();
      defaultMaxDeliveryAttempts =
         ((Integer) ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();      
   }

   protected void tearDown() throws Exception
   {
      if (ic != null) ic.close();

      ServerManagement.undeployQueue(DLQ_NAME);
      ServerManagement.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

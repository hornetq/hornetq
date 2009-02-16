/*
 * JORAM: Java(TM) Open Reliable Asynchronous Messaging
 * Copyright (C) 2002 INRIA
 * Contact: joram-team@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 * 
 * Initial developer(s): Jeff Mesnil (jmesnil@gmail.com)
 * Contributor(s): ______________________________________.
 */

package org.objectweb.jtests.jms.conform.session;

import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jboss.util.NestedRuntimeException;
import org.objectweb.jtests.jms.framework.UnifiedTestCase;

/**
 * Test unified JMS 1.1 sessions.
 * <br />
 * See JMS 1.1 specifications
 * 
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: UnifiedSessionTest.java,v 1.1 2007/03/29 04:28:37 starksm Exp $
 * @since JMS 1.1
 */
public class UnifiedSessionTest extends UnifiedTestCase
{

   /**
    * QueueConnection
    */
   protected QueueConnection queueConnection;

   /**
    * QueueSession (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected QueueSession queueSession;

   /**
    * TopicConnection
    */
   protected TopicConnection topicConnection;

   /**
    * TopicSession (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected TopicSession topicSession;

   /**
    * Test that a call to <code>createDurableConnectionConsumer()</code> method 
    * on a <code>QueueConnection</code> throws a 
    * <code>javax.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateDurableConnectionConsumerOnQueueConnection()
   {
      try
      {
         queueConnection.createDurableConnectionConsumer(topic, "subscriptionName", "", (ServerSessionPool) null, 1);
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createDurableSubscriber()</code> method 
    * on a <code>QueueSession</code> throws a 
    * <code>javax.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateDurableSubscriberOnQueueSession()
   {
      try
      {
         queueSession.createDurableSubscriber(topic, "subscriptionName");
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createTemporaryTopic()</code> method 
    * on a <code>QueueSession</code> throws a 
    * <code>javax.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateTemporaryTopicOnQueueSession()
   {
      try
      {
         queueSession.createTemporaryTopic();
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createTopic()</code> method 
    * on a <code>QueueSession</code> throws a 
    * <code>javax.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateTopicOnQueueSession()
   {
      try
      {
         queueSession.createTopic("topic_name");
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>unsubscribe()</code> method 
    * on a <code>QueueSession</code> throws a 
    * <code>javax.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testUnsubscribeOnQueueSession()
   {
      try
      {
         queueSession.unsubscribe("subscriptionName");
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createBrowser()</code> method 
    * on a <code>TopicSession</code> throws a 
    * <code>javax.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateBrowserOnTopicSession()
   {
      try
      {
         topicSession.createBrowser(queue);
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createQueue()</code> method 
    * on a <code>TopicSession</code> throws a 
    * <code>javax.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateQueueOnTopicSession()
   {
      try
      {
         topicSession.createQueue("queue_name");
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
   }

   /**
    * Test that a call to <code>createTemporaryQueue()</code> method 
    * on a <code>TopicSession</code> throws a 
    * <code>javax.jms.IllegalStateException</code>.
    * (see JMS 1.1 specs, table 4-1).
    * 
    * @since JMS 1.1
    */
   public void testCreateTemporaryQueueOnTopicSession()
   {
      try
      {
         topicSession.createTemporaryQueue();
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
   }

   public void setUp() throws Exception
   {
      super.setUp();
      try
      {
         queueConnection = queueConnectionFactory.createQueueConnection();
         queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         topicConnection = topicConnectionFactory.createTopicConnection();
         topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         queueConnection.start();
         topicConnection.start();
      }
      catch (Exception e)
      {
         throw new NestedRuntimeException(e);
      }
   }

   public void tearDown() throws Exception
   {
      try
      {
         queueConnection.close();
         topicConnection.close();
      }
      catch (Exception ignored)
      {
      }
      finally
      {
         queueConnection = null;
         queueSession = null;
         topicConnection = null;
         topicSession = null;
         super.tearDown();
      }
   }

   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(UnifiedSessionTest.class);
   }

   public UnifiedSessionTest(String name)
   {
      super(name);
   }
}

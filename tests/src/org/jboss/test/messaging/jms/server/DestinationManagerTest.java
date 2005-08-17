/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.server;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.DestinationManagerImpl;

import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.JMSException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DestinationManagerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   protected DestinationManager destinationManager;

   // Constructors --------------------------------------------------

   public DestinationManagerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.startInVMServer();
      initialContext = new InitialContext();

      destinationManager = ServerManagement.getServerPeer().getDestinationManager();

   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      destinationManager = null;
      super.tearDown();
   }

   public void testCreateQueue() throws Exception
   {
      String name = "testQueue";
      destinationManager.createQueue(name, null);

      Queue q = (Queue)initialContext.lookup(DestinationManager.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
   }

   public void testCreateTopic() throws Exception
   {
      String name = "testQueue";
      destinationManager.createTopic(name, null);

      Topic t = (Topic)initialContext.lookup(DestinationManager.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());
   }

   public void testCreateQueueDifferentJNDIName() throws Exception
   {
      String name = "testQueue";
      String jndiName = "/a/b/c/testQueue2";
      destinationManager.createQueue(name, jndiName);

      Queue q = (Queue)initialContext.lookup(jndiName);
      assertEquals(name, q.getQueueName());
   }

   public void testCreateQueueDifferentJNDIName2() throws Exception
   {
      String name = "testQueue";
      String jndiName = "testQueue";
      destinationManager.createQueue(name, jndiName);

      Queue q = (Queue)initialContext.lookup(jndiName);
      assertEquals(name, q.getQueueName());
   }


   public void testCreateTopicDifferrentJNDIName() throws Exception
   {
      String name = "testTopic";
      String jndiName = "/a/b/c/testTopic2";
      destinationManager.createTopic(name, jndiName);

      Topic t = (Topic)initialContext.lookup(jndiName);
      assertEquals(name, t.getTopicName());
   }

   public void testCreateDuplicateQueue() throws Exception
   {
      String name = "testQueue";
      destinationManager.createQueue(name, null);

      try
      {
         destinationManager.createQueue(name, null);
         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }

   public void testCreateDuplicateTopic() throws Exception
   {
      String name = "testTopic";
      destinationManager.createTopic(name, null);

      try
      {
         destinationManager.createTopic(name, null);
         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }

   public void testCreateDuplicateQueueDifferentJNDIName() throws Exception
   {
      String name = "testQueue";
      destinationManager.createQueue(name, null);

      try
      {
         destinationManager.createQueue(name, "x/y/z/testQueueA");

         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }

   public void testCreateDuplicateTopicDifferentJNDIName() throws Exception
   {
      String name = "testTopic";
      destinationManager.createTopic(name, null);

      try
      {
         destinationManager.createTopic(name, "x/y/z/testTopicA");
         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }


   public void testCreateQueueAndTopicWithTheSameName() throws Exception
   {
      String name = "SomeName";
      destinationManager.createQueue(name, null);
      destinationManager.createTopic(name, null);

      Queue q = (Queue)initialContext.lookup(DestinationManager.DEFAULT_QUEUE_CONTEXT + "/" + name);
      Topic t = (Topic)initialContext.lookup(DestinationManager.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
      assertEquals(name, t.getTopicName());
   }

   public void testDestroyInexistentQueue() throws Exception
   {
      destinationManager.destroyQueue("there is not such a queue");
   }

   public void testDestroyInexistentTopic() throws Exception
   {
      destinationManager.destroyTopic("there is not such a topic");
   }


   public void testDestroyQueue() throws Exception
   {
      String name = "testQueue";
      destinationManager.createQueue(name, null);

      Queue q = (Queue)initialContext.lookup(DestinationManager.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());

      destinationManager.destroyQueue(name);

      assertNull(((DestinationManagerImpl)destinationManager).getCoreDestination(q));

      try
      {
         Object o = initialContext.lookup(DestinationManager.DEFAULT_QUEUE_CONTEXT + "/" + name);
         fail("should have thrown exception, but got " + o);
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }

   public void testDestroyTopic() throws Exception
   {
      String name = "testTopic";
      destinationManager.createTopic(name, null);

      Topic t = (Topic)initialContext.lookup(DestinationManager.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());

      destinationManager.destroyTopic(name);

      assertNull(((DestinationManagerImpl)destinationManager).getCoreDestination(t));

      try
      {
         Object o = initialContext.lookup(DestinationManager.DEFAULT_TOPIC_CONTEXT + "/" + name);
         fail("should have thrown exception but got " + o);
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }




   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

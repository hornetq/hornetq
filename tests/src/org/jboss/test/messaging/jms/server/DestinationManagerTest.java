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

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

import org.jboss.jms.server.DestinationManagerImpl;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

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

   // Constructors --------------------------------------------------

   public DestinationManagerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());

      //destinationManager = ServerManagement.getServerPeer().getDestinationManager();
      ServerManagement.undeployQueue("testQueue");
      ServerManagement.undeployTopic("testTopic");
      ServerManagement.undeployQueue("SomeName");
      ServerManagement.undeployTopic("SomeName");

   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("testQueue");
      ServerManagement.undeployTopic("testTopic");
      ServerManagement.undeployQueue("SomeName");
      ServerManagement.undeployTopic("SomeName");
      
      
      //destinationManager = null;
      super.tearDown();
      
      
   }

   public void testCreateQueue() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      String name = "testQueue";
      this.createQueue(name, null);

      Queue q = (Queue)initialContext.lookup(DestinationManagerImpl.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
   }

   public void testCreateTopic() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testQueue";
      this.createTopic(name, null);

      Topic t = (Topic)initialContext.lookup(DestinationManagerImpl.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());
   }

   public void testCreateQueueDifferentJNDIName() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testQueue";
      String jndiName = "/a/b/c/testQueue2";
      this.createQueue(name, jndiName);

      Queue q = (Queue)initialContext.lookup(jndiName);
      assertEquals(name, q.getQueueName());
   }

   public void testCreateQueueDifferentJNDIName2() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testQueue";
      String jndiName = "testQueue";
      this.createQueue(name, jndiName);

      Queue q = (Queue)initialContext.lookup(jndiName);
      assertEquals(name, q.getQueueName());
   }


   public void testCreateTopicDifferrentJNDIName() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testTopic";
      String jndiName = "/a/b/c/testTopic2";
      this.createTopic(name, jndiName);

      Topic t = (Topic)initialContext.lookup(jndiName);
      assertEquals(name, t.getTopicName());
   }

   public void testCreateDuplicateQueue() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testQueue";
      this.createQueue(name, null);

      try
      {
         this.createQueue(name, null);
         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }

   public void testCreateDuplicateTopic() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testTopic";
      this.createTopic(name, null);

      try
      {
         this.createTopic(name, null);
         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }

   public void testCreateDuplicateQueueDifferentJNDIName() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testQueue";
      this.createQueue(name, null);

      try
      {
         this.createQueue(name, "x/y/z/testQueueA");

         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }

   public void testCreateDuplicateTopicDifferentJNDIName() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testTopic";
      this.createTopic(name, null);

      try
      {
         this.createTopic(name, "x/y/z/testTopicA");
         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }


   public void testCreateQueueAndTopicWithTheSameName() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "SomeName";
      this.createQueue(name, null);
      this.createTopic(name, null);

      Queue q = (Queue)initialContext.lookup(DestinationManagerImpl.DEFAULT_QUEUE_CONTEXT + "/" + name);
      Topic t = (Topic)initialContext.lookup(DestinationManagerImpl.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
      assertEquals(name, t.getTopicName());
   }

   public void testDestroyInexistentQueue() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      this.destroyQueue("there is not such a queue");
   }

   public void testDestroyInexistentTopic() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      this.destroyTopic("there is not such a topic");
   }


   public void testDestroyQueue() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testQueue";
      this.createQueue(name, null);

      Queue q = (Queue)initialContext.lookup(DestinationManagerImpl.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());

      this.destroyQueue(name);

      //assertNull(((DestinationManager)destinationManager).getCoreDestination(q));

      try
      {
         Object o = initialContext.lookup(DestinationManagerImpl.DEFAULT_QUEUE_CONTEXT + "/" + name);
         fail("should have thrown exception, but got " + o);
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }

   public void testDestroyTopic() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      String name = "testTopic";
      this.createTopic(name, null);

      Topic t = (Topic)initialContext.lookup(DestinationManagerImpl.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());

      this.destroyTopic(name);

      //assertNull(((DestinationManager)destinationManager).getCoreDestination(t));

      try
      {
         Object o = initialContext.lookup(DestinationManagerImpl.DEFAULT_TOPIC_CONTEXT + "/" + name);
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
   
   private void createQueue(String name, String jndiName)
      throws Exception
   {
      ServerManagement.deployQueue(name, jndiName);
   }
   
   private void createTopic(String name, String jndiName)
      throws Exception
   {
      ServerManagement.deployTopic(name, jndiName);
   }
   
   private void destroyQueue(String name)
      throws Exception
   {
      ServerManagement.undeployQueue(name);
   }
   
   private void destroyTopic(String name)
      throws Exception
   {
      ServerManagement.undeployTopic(name);
   }
   
   // Inner classes -------------------------------------------------   
}

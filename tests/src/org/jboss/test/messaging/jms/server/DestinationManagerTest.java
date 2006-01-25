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

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

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
      if (ServerManagement.isRemote())
      {
         fail("this test is not supposed to run in a remote configuration!");
      }

      super.setUp();
      ServerManagement.start("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());

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
      
      super.tearDown();
   }

   public void testCreateQueue() throws Exception
   {
      String name = "testQueue";

      ServerManagement.deployQueue(name, null);

      Queue q = (Queue)initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
   }

   public void testCreateTopic() throws Exception
   {
      String name = "testQueue";

      ServerManagement.deployTopic(name, null);

      Topic t = (Topic)initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());
   }

   public void testCreateQueueDifferentJNDIName() throws Exception
   {
      String name = "testQueue";
      String jndiName = "/a/b/c/testQueue2";

      ServerManagement.deployQueue(name, jndiName);

      Queue q = (Queue)initialContext.lookup(jndiName);
      assertEquals(name, q.getQueueName());
   }

   public void testCreateQueueDifferentJNDIName2() throws Exception
   {
      String name = "testQueue";
      String jndiName = "testQueue";

      ServerManagement.deployQueue(name, jndiName);

      Queue q = (Queue)initialContext.lookup(jndiName);
      assertEquals(name, q.getQueueName());
   }


   public void testCreateTopicDifferrentJNDIName() throws Exception
   {
      String name = "testTopic";
      String jndiName = "/a/b/c/testTopic2";

      ServerManagement.deployTopic(name, jndiName);

      Topic t = (Topic)initialContext.lookup(jndiName);
      assertEquals(name, t.getTopicName());
   }

   public void testCreateDuplicateQueue() throws Exception
   {
      String name = "testQueue";

      ServerManagement.deployQueue(name, null);

      try
      {
         ServerManagement.deployQueue(name, null);
         fail("should have thrown exception");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testCreateDuplicateTopic() throws Exception
   {
      String name = "testTopic";

      ServerManagement.deployTopic(name, null);

      try
      {
         ServerManagement.deployTopic(name, null);
         fail("should have thrown exception");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testCreateDuplicateQueueDifferentJNDIName() throws Exception
   {
      String name = "testQueue";

      ServerManagement.deployQueue(name, null);

      try
      {
         ServerManagement.deployQueue(name, "x/y/z/testQueueA");

         fail("should have thrown exception");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testCreateDuplicateTopicDifferentJNDIName() throws Exception
   {
      String name = "testTopic";

      ServerManagement.deployTopic(name, null);

      try
      {
         ServerManagement.deployTopic(name, "x/y/z/testTopicA");
         fail("should have thrown exception");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testCreateQueueAndTopicWithTheSameName() throws Exception
   {
      String name = "SomeName";

      ServerManagement.deployQueue(name, null);

      ServerManagement.deployTopic(name, null);

      Queue q = (Queue)initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);
      Topic t = (Topic)initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
      assertEquals(name, t.getTopicName());
   }

   public void testDestroyInexistentQueue() throws Exception
   {
      ServerManagement.undeployQueue("there is not such a queue");
   }

   public void testDestroyInexistentTopic() throws Exception
   {
      ServerManagement.undeployTopic("there is not such a topic");
   }

   public void testDestroyQueue() throws Exception
   {
      String name = "testQueue";

      ServerManagement.deployQueue(name, null);

      Queue q = (Queue)initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());

      ServerManagement.undeployQueue(name);

      //assertNull(((DestinationManager)destinationManager).getCoreDestination(q));

      try
      {
         Object o = initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);
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

      ServerManagement.deployTopic(name, null);

      Topic t = (Topic)initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());

      ServerManagement.undeployTopic(name);

      //assertNull(((DestinationManager)destinationManager).getCoreDestination(t));

      try
      {
         Object o = initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);
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

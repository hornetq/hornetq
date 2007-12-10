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

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.destination.ManagedDestination;
import org.jboss.jms.server.destination.ManagedQueue;
import org.jboss.jms.server.destination.ManagedTopic;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DestinationManagerTest extends JBMServerTestCase
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
            
      initialContext = getInitialContext();

      undeployQueue("aTestQueue");
      undeployTopic("aTestTopic");
      undeployQueue("SomeName");
      undeployTopic("SomeName");

   }

   public void tearDown() throws Exception
   {
      undeployQueue("aTestQueue");
      undeployTopic("aTestTopic");
      undeployQueue("SomeName");
      undeployTopic("SomeName");
      
      super.tearDown();
   }

   public void testDeployQueue() throws Exception
   {
      String name = "aTestQueue";

      deployQueue(name, null);

      Queue q = (Queue)initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
   }

   public void testDeployTopic() throws Exception
   {
      String name = "aTestQueue";

      deployTopic(name, null);

      Topic t = (Topic)initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());
   }

   public void testDeployQueueDifferentJNDIName() throws Exception
   {
      String name = "aTestQueue";
      String jndiName = "/a/b/c/aTestQueue2";

      deployQueue(name, jndiName);

      Queue q = (Queue)initialContext.lookup(jndiName);
      assertEquals(name, q.getQueueName());
   }

   public void testDeployQueueDifferentJNDIName2() throws Exception
   {
      String name = "aTestQueue";
      String jndiName = "aTestQueue";

      deployQueue(name, jndiName);

      Queue q = (Queue)initialContext.lookup(jndiName);
      assertEquals(name, q.getQueueName());
   }


   public void testDeployTopicDifferrentJNDIName() throws Exception
   {
      String name = "aTestTopic";
      String jndiName = "/a/b/c/aTestTopic2";

      deployTopic(name, jndiName);

      Topic t = (Topic)initialContext.lookup(jndiName);
      assertEquals(name, t.getTopicName());
   }

   public void testDeployDuplicateQueue() throws Exception
   {
      String name = "aTestQueue";

      deployQueue(name, null);

      try
      {
         deployQueue(name, null);
         fail("should have thrown exception");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testDeployDuplicateTopic() throws Exception
   {
      String name = "aTestTopic";

      deployTopic(name, null);

      try
      {
         deployTopic(name, null);
         fail("should have thrown exception");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testDeployDuplicateQueueDifferentJNDIName() throws Exception
   {
      String name = "aTestQueue";

      deployQueue(name, null);

      try
      {
         deployQueue(name, "x/y/z/aTestQueueA");

         fail("should have thrown exception");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testDeployDuplicateTopicDifferentJNDIName() throws Exception
   {
      String name = "aTestTopic";

      deployTopic(name, null);

      try
      {
         deployTopic(name, "x/y/z/aTestTopicA");
         fail("should have thrown exception");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testDeployQueueAndTopicWithTheSameName() throws Exception
   {
      String name = "SomeName";

      deployQueue(name, null);

      deployTopic(name, null);

      Queue q = (Queue)initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);
      Topic t = (Topic)initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
      assertEquals(name, t.getTopicName());
   }

   public void testUndeployQueue() throws Exception
   {
      String name = "aTestQueue";

      deployQueue(name, null);

      Queue q = (Queue)initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());

      undeployQueue(name);

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

   public void testUndeployTopic() throws Exception
   {
      String name = "aTestTopic";

      deployTopic(name, null);

      Topic t = (Topic)initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());

      undeployTopic(name);

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

   public void testCreateQueueProgramatically() throws Exception
   {
      String name = "SomeQueue";

      deployQueue(name, null);
      Queue q = (Queue)initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);

      assertEquals(name, q.getQueueName());
   }

   public void testCreateTopicProgramatically() throws Exception
   {
      String name = "SomeTopic";

      deployTopic(name, null);
      Topic t = (Topic)initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);

      assertEquals(name, t.getTopicName());
   }

   public void testUndeployInexistentQueue() throws Exception
   {
      undeployQueue("there is not such a queue");
   }

   public void testUndeployInexistentTopic() throws Exception
   {
      undeployTopic("there is not such a topic");
   }

   public void testDestroyQueue() throws Exception
   {
      String name = "AnotherQueue";
      deployQueue(name, null);
      assertTrue(destroyQueue(name));

      try
      {
         initialContext.lookup(ServerManagement.DEFAULT_QUEUE_CONTEXT + "/" + name);
         fail("should have failed");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }

   public void testDestroyTopic() throws Exception
   {
      String name = "AnotherTopic";
      deployTopic(name, null);
      assertTrue(destroyTopic(name));

      try
      {
         initialContext.lookup(ServerManagement.DEFAULT_TOPIC_CONTEXT + "/" + name);
         fail("should have failed");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }

   public void testDestroyInexistentQueue() throws Exception
   {
      assertFalse(destroyQueue("NoSuchQueue"));
   }

   public void testDestroyInexistentTopic() throws Exception
   {
      assertFalse(destroyTopic("NoSuchTopic"));
   }



   
   public void testDestinationManager() throws Exception
   {
      ServerPeer sp = (ServerPeer) getJmsServer();
      
      DestinationManager dm = sp.getDestinationManager();
      
      stopDestinationManager();
      
      startDestinationManager();
        
      ManagedQueue queue1 = new ManagedQueue("queue1", 1000, 10, 10, false);
      
      ManagedTopic topic1 = new ManagedTopic("topic1", 1000, 10, 10, false);
      
      dm.registerDestination(queue1);
      
      dm.registerDestination(topic1);
      
      ManagedDestination queue2 = dm.getDestination("not exists", true);
      
      assertNull(queue2);
      
      ManagedDestination topic2 = dm.getDestination("not exists", false);
      
      assertNull(topic2);
      
      ManagedQueue queue3 = (ManagedQueue)dm.getDestination("queue1", true);
      
      assertTrue(queue1 == queue3);
      
      ManagedDestination queue4 = dm.getDestination("queue1", false);
      
      assertNull(queue4);
      
      ManagedTopic topic3 = (ManagedTopic)dm.getDestination("topic1", false);
      
      assertTrue(topic1 == topic3);
      
      ManagedDestination topic4 = dm.getDestination("topic1", true);
      
      assertNull(topic4);            
            
      dm.unregisterDestination(queue1);
      
      ManagedDestination queue5 = dm.getDestination("queue1", true);
      
      assertNull(queue5);
      
      dm.unregisterDestination(topic1);
      
      ManagedDestination topic5 = dm.getDestination("topic1", false);
      
      assertNull(topic5);
      
      dm.registerDestination(queue1);
      
      dm.registerDestination(topic1);
      //we need to stop the server so evertything can redeploy
      stop();
   }



   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}

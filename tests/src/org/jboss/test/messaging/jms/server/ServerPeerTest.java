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
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import org.jboss.jms.util.MessagingJMSException;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerPeerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public ServerPeerTest(String name)
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
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testNonContextAlreadyBound() throws Exception
   {
      ServerManagement.stopServerPeer();

      initialContext.bind("/some-new-context", new Object());

      try
      {
         ServerManagement.startServerPeer(0, "/some-new-context", null);
         fail("should throw exception");
      }
      catch(MessagingJMSException e)
      {
         // OK
      }
   }

   public void testChangeDefaultJNDIContexts() throws Exception
   {
      ServerManagement.stopServerPeer();
     
      ServerManagement.startServerPeer(0, "/here-go-queues", "/and-here-topics/etc/etc");
      
      try
      {
         ServerManagement.deployQueue("SomeQueue");
         ServerManagement.deployTopic("SomeTopic");


         Queue q = (Queue)initialContext.lookup("/here-go-queues/SomeQueue");
         Topic t = (Topic)initialContext.lookup("/and-here-topics/etc/etc/SomeTopic");

         assertEquals("SomeQueue", q.getQueueName());
         assertEquals("SomeTopic", t.getTopicName());

      }
      finally
      {
         ServerManagement.undeployQueue("SomeQueue");
         ServerManagement.undeployTopic("SomeTopic");
         ServerManagement.stopServerPeer();
      }
   }

   public void testUnbindContexts() throws Exception
   {

      if(!ServerManagement.isServerPeerStarted())
      {
         ServerManagement.startServerPeer();
      }

      Context c = (Context)initialContext.lookup("/queue");
      c = (Context)initialContext.lookup("/topic");

      log.trace("context: " + c);

      ServerManagement.stopServerPeer();

      try
      {
         c = (Context)initialContext.lookup("/queue");
         fail("this should fail");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      try
      {
         c = (Context)initialContext.lookup("/topic");
         fail("this should fail");
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

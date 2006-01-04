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
package org.jboss.test.messaging.core.distributed.queue.base;

import org.jboss.messaging.core.distributed.queue.DistributedQueue;
import org.jboss.messaging.core.distributed.queue.DistributedQueue;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.distributed.base.DistributedChannelTestBase;

import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DistributedQueueTestBase extends DistributedChannelTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DistributedQueueTestBase(String name)
   {
      super(name);
   }


   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   /**
    * Three a distributed queue with three peers
    */
   
   public void testSimpleSend() throws Exception
   {
      jchannel2.connect("testGroup");
      jchannel3.connect("testGroup");

      // allow the group time to form
      Thread.sleep(2000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());
      assertTrue(jchannel3.isConnected());

      // make sure all three jchannels joined the group
      assertEquals(3, jchannel.getView().getMembers().size());
      assertEquals(3, jchannel2.getView().getMembers().size());
      assertEquals(3, jchannel3.getView().getMembers().size());

      DistributedQueue peer = (DistributedQueue)channel;
      DistributedQueue peer2 = (DistributedQueue)channel2;
      DistributedQueue peer3 = (DistributedQueue)channel3;

      SimpleReceiver r2 = new SimpleReceiver("receiver2", SimpleReceiver.ACKING);
      SimpleReceiver r3 = new SimpleReceiver("receiver3", SimpleReceiver.ACKING);

      peer2.add(r2);
      peer3.add(r3);

      peer.join();
      peer2.join();
      peer3.join();

      // send a non-reliable message
      Message m = MessageFactory.createMessage("message0", false, "payload");
      assertTrue(peer.handle(null, m, null).isDone());

      assertTrue(peer.browse().isEmpty());

      List emptyList = r2.getMessages();
      List messageList = r3.getMessages();

      if (messageList.isEmpty())
      {
         List tmp = emptyList;
         emptyList = messageList;
         messageList = tmp;
      }

      assertTrue(emptyList.isEmpty());
      assertEquals(1, messageList.size());
      assertEquals("message0", ((Message)messageList.get(0)).getMessageID());
      assertEquals("payload", ((Message)messageList.get(0)).getPayload());

   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

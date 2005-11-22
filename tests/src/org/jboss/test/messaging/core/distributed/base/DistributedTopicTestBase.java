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
package org.jboss.test.messaging.core.distributed.base;

import org.jboss.test.messaging.core.distributed.JGroupsUtil;
import org.jboss.test.messaging.core.local.base.TopicTestBase;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.distributed.topic.DistributedTopic;
import org.jboss.messaging.core.distributed.topic.DistributedTopic;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;

import java.util.List;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DistributedTopicTestBase extends TopicTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected JChannel jchannel, jchannel2, jchannel3;
   protected RpcDispatcher dispatcher, dispatcher2, dispatcher3;

   protected DistributedTopic topic2, topic3;

   // Constructors --------------------------------------------------

   public DistributedTopicTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      jchannel = new JChannel(JGroupsUtil.generateProperties(50, 1));
      jchannel2 = new JChannel(JGroupsUtil.generateProperties(900000, 1));
      jchannel3 = new JChannel(JGroupsUtil.generateProperties(900000, 2));

      dispatcher = new RpcDispatcher(jchannel, null, null, new RpcServer("1"));
      dispatcher2 = new RpcDispatcher(jchannel2, null, null, new RpcServer("2"));
      dispatcher3 = new RpcDispatcher(jchannel3, null, null, new RpcServer("3"));

      // connect only the first JChannel
      jchannel.connect("testGroup");

      assertEquals(1, jchannel.getView().getMembers().size());
   }

   public void tearDown() throws Exception
   {
      jchannel.close();
      jchannel2.close();
      jchannel3.close();

      super.tearDown();
   }


   public void testUnreliableMessageTwoRemoteReceivers() throws Exception
   {
      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((DistributedTopic)topic).join();
      topic2.join();

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);

      topic.add(r1);
      topic2.add(r2);

      Message m = MessageFactory.createMessage("message0", false, "payload");
      Delivery d = topic.handle(observer, ms.reference(m), null);
      log.debug("message sent");

      assertTrue(d.isDone());

      List l1 = r1.getMessages();
      List l2 = r2.getMessages();

      assertEquals(1, l1.size());
      assertEquals("payload", ((Message)l1.get(0)).getPayload());

      assertEquals(1, l2.size());
      assertEquals("payload", ((Message)l2.get(0)).getPayload());
   }

   public void testReliableMessageTwoRemoteReceivers() throws Exception
   {
      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((DistributedTopic)topic).join();
      topic2.join();

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);

      topic.add(r1);
      topic2.add(r2);

      Message m = MessageFactory.createMessage("message0", true, "payload");
      Delivery d = topic.handle(observer, ms.reference(m), null);
      log.debug("message sent");

      assertTrue(d.isDone());

      List l1 = r1.getMessages();
      List l2 = r2.getMessages();

      assertEquals(1, l1.size());
      assertEquals("payload", ((Message)l1.get(0)).getPayload());

      assertEquals(1, l2.size());
      assertEquals("payload", ((Message)l2.get(0)).getPayload());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

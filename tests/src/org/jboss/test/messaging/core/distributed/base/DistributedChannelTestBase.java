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


import org.jboss.test.messaging.core.base.ChannelTestBase;
import org.jboss.test.messaging.core.distributed.JGroupsUtil;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.SimpleFilter;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.tx.Transaction;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;

import java.util.List;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DistributedChannelTestBase extends ChannelTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected JChannel jchannel, jchannel2, jchannel3;
   protected RpcDispatcher dispatcher, dispatcher2, dispatcher3;

   protected MessageStore ms2, ms3;
   protected Channel channel2, channel3;

   // Constructors --------------------------------------------------

   public DistributedChannelTestBase(String name)
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

   public void testJGroupsChannelNotConnected() throws Exception
   {
      assertTrue(jchannel.isConnected());
      jchannel.close();

      try
      {
         ((Distributed)channel).join();
         fail("should throw DistributedException");
      }
      catch(DistributedException e)
      {
         //OK
      }
   }

   //////////////////////////////////
   ////////////////////////////////// Test matrix
   //////////////////////////////////

   //
   // Standard Channel tests, but with receivers attached to a remote peer
   //

   //
   // Non-recoverable channel
   //

   ////
   //// Zero receivers
   ////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      log.debug(channel + " browse done");
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      log.debug(channel2 + " browse done");
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_4() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_6() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }



   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_7() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_8() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_9() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_10() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_12() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableDistributedChannel_12_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }


   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_13() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_14() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_15() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_16() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableDistributedChannel_16_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   ////
   //// One receiver
   ////

   //////
   ////// ACKING receiver
   //////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   
   
   public void testNonRecoverableDistributedChannel_17() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());
      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_18() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List received = r.getMessages();
      assertEqualSets(messages, received);
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_19_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_20_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
      assertEqualSets(messages, r.getMessages());
   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_21() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_22() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
      assertEqualSets(messages, r.getMessages());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_23_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_24_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      assertEqualSets(messages, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableDistributedChannel_24_2_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      assertEqualSets(messages, r.getMessages());
   }

   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_25() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));


      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_26() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());


      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   // test covered by ChannelTestBase

   //////////
   ////////// Multiple messages
   //////////

   // test covered by ChannelTestBase

   //////
   ////// NACKING receiver
   //////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testNonRecoverableDistributedChannel_28() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   /**
    * The same test as before, but with a Receiver configured to acknowledge immediately
    * on the Delivery. Simulates a race condition in which the acknoledgment arrives before
    * the Delivery is returned to channel.
    *
    * @throws Throwable
    */
   public void testNonRecoverableDistributedChannel_28_race() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals("message0", ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testNonRecoverableDistributedChannel_29() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testNonRecoverableDistributedChannel_30() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }


   //////////
   ////////// Multiple messages
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testNonRecoverableDistributedChannel_31() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testNonRecoverableDistributedChannel_32() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testNonRecoverableDistributedChannel_33() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      tx.rollback();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_34() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals("message0", rm.getMessageID());

      r.acknowledge(rm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment
   ////////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // Doesn't make sense, the message won't be accepted anyway.

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_35() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals("message0", rm.getMessageID());

      Transaction tx = tr.createTransaction();

      r.acknowledge(rm, tx);

      stored = channel.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_37() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel2.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment
   ////////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // Doesn't make sense, the message won't be accepted anyway.

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_38() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_39() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_40() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel does accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_42() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals("message0", rm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // test covered by ChannelTestBase

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableDistributedChannel_44() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableDistributedChannel_44_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());
   }

   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_45() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_46() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableDistributedChannel_47() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableDistributedChannel_48() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableDistributedChannel_48_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //
   // Recoverable channel
   //

   ////
   //// Zero receivers
   ////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_1() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_2() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_3() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      // TODO find a way to find out whether the reference was cleared or not
//      // cleanup references and garbage collect
//      stored = null;
//      sm = null;
//      System.gc();
//
//      int cnt =((JDBCTransactionLog)msTransactionLogDelegate).
//            getMessageReferenceCount(m.getMessageID());
//
//      assertEquals(1, cnt);
   }

   //////////
   ////////// Multiple messagess
   //////////

   public void testRecoverableDistributedChannel_4() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_5() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_6() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_7() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_8() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableDistributedChannel_8_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
   }

   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_9() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_10() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_11() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_12() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableDistributedChannel_12_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no remote receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());
   }

   ////
   //// One receiver
   ////

   //////
   ////// ACKING receiver
   //////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_13() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());
      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_14() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List received = r.getMessages();
      assertEqualSets(messages, received);
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_15() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_16() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
      assertEqualSets(messages, r.getMessages());
   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_17() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_18() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
      assertEqualSets(messages, r.getMessages());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_19() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_20() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      assertEqualSets(messages, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableDistributedChannel_20_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      assertEqualSets(messages, r.getMessages());
   }

   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_21() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_22() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_23() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_24() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableDistributedChannel_24_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an ACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////
   ////// NACKING receiver
   //////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableDistributedChannel_25() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   /**
    * The same test as before, but with a Receiver configured to acknowledge immediately
    * on the Delivery. Simulates a race condition in which the acknoledgment arrives before
    * the Delivery is returned to channel.
    */
   public void testRecoverableDistributedChannel_25_race() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals("message0", ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableDistributedChannel_25_1() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableDistributedChannel_25_2() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());


      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableDistributedChannel_26() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableDistributedChannel_26_1() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableDistributedChannel_26_2() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      tx.rollback();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableDistributedChannel_27() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());


      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   /**
    * The same test as before, but with a Receiver configured to acknowledge immediately
    * on the Delivery. Simulates a race condition in which the acknoledgment arrives before
    * the Delivery is returned to channel.
    *
    * @throws Throwable
    */
   public void testRecoverableDistributedChannel_27_race() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

            jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals("message0", ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableDistributedChannel_27_1() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableDistributedChannel_27_2() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableDistributedChannel_28() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableDistributedChannel_28_1() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableDistributedChannel_28_2() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      tx.rollback();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel.browse().isEmpty());
   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_29() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_30() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_31() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());
      delivering = channel2.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_32() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableDistributedChannel_32_mixed() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, channel2.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());
   }

   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_33() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_34() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableDistributedChannel_35() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableDistributedChannel_36() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = MessageFactory.createCoreMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableDistributedChannel_36_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // add an NACKING receiver to the remote channel peer
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel2.add(r));

      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = MessageFactory.createCoreMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());
      assertEquals(0, channel2.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////////////////////////
   /////////////////////////////// Add receiver tests
   ///////////////////////////////

   //
   // Non-recoverable channel
   //

   ////
   //// Non-reliable message
   ////

   //////
   ////// Broken receiver
   //////

   public void testAddReceiver_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      stored = channel2.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver = new SimpleReceiver("BrokenReceiver", SimpleReceiver.BROKEN);
      assertTrue(channel2.add(receiver));

      stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      assertTrue(receiver.getMessages().isEmpty());
   }

   //////
   ////// ACKING receiver
   //////

   public void testAddReceiver_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      log.debug("sending message");

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);

      log.debug("message sent");

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      stored = channel2.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING, channel2);
      assertTrue(channel2.add(receiver));

      log.debug("receiver added");

      assertEquals(1, channel.browse().size());
      assertEquals(1, channel2.browse().size());

      log.debug("requesting message");

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////
   ////// NACKING receiver
   //////

   public void testAddReceiver_3() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      stored = channel2.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("NACKINGReceiver", SimpleReceiver.NACKING, channel2);
      assertTrue(channel2.add(receiver));

      assertEquals(1, channel.browse().size());
      assertEquals(1, channel2.browse().size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertEquals(1, channel.browse().size());
      assertEquals(1, channel2.browse().size());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      receiver.acknowledge(sm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      messages = receiver.getMessages();
      assertEquals(1, messages.size());
      sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //
   // Recoverable channel
   //

   ////
   //// Reliable message
   ////

   public void testAddReceiver_4() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      stored = channel2.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver = new SimpleReceiver("BrokenReceiver", SimpleReceiver.BROKEN);
      assertTrue(channel2.add(receiver));

      stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
      stored = channel2.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      assertTrue(receiver.getMessages().isEmpty());
   }

   //////
   ////// ACKING receiver
   //////

   public void testAddReceiver_5() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      stored = channel2.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("ACKINGReceiver", SimpleReceiver.ACKING, channel2);
      assertTrue(channel2.add(receiver));

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////
   ////// NACKING receiver
   //////

   public void testAddReceiver_6() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      stored = channel2.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("NACKINGReceiver", SimpleReceiver.NACKING, channel2);
      assertTrue(channel2.add(receiver));

      assertEquals(1, channel.browse().size());
      assertEquals(1, channel2.browse().size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertEquals(1, channel.browse().size());
      assertEquals(1, channel2.browse().size());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      receiver.acknowledge(sm, null);

      assertTrue(channel.browse().isEmpty());
      assertTrue(channel2.browse().isEmpty());

      messages = receiver.getMessages();
      assertEquals(1, messages.size());
      sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   ///////////////////////////////
   /////////////////////////////// Forwarding
   ///////////////////////////////

   //
   // Non-recoverable channel
   //

   ////
   //// Non-reliable message
   ////

   // TODO: Stopped working on this while suspecting a JGroups deadlock due to nested synchronous
   //       remote calls. Uncomment and make the test pass when starting to work on the distributed
   //       part again

//   public void testForwarding_1() throws Exception
//   {
//      if (channel.isRecoverable())
//      {
//         // we test only non-recoverable channels now
//         return;
//      }
//
//      jchannel2.connect("testGroup");
//
//      // allow the group time to form
//      Thread.sleep(1000);
//
//      assertTrue(jchannel.isConnected());
//      assertTrue(jchannel2.isConnected());
//
//      // make sure both jchannels joined the group
//      assertEquals(2, jchannel.getView().getMembers().size());
//      assertEquals(2, jchannel2.getView().getMembers().size());
//
//      ((Distributed)channel).join();
//
//      // send a message that will be stored locally
//      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
//      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
//
//      // non-recoverable channel, non-reliable message
//      log.debug("sending message");
//      Delivery delivery = channel.handle(observer, m, null);
//      log.debug("message sent");
//
//      assertTrue(delivery.isDone());
//
//      assertEquals(1, channel.browse().size());
//
//      // the second peer joins
//
//      ((Distributed)channel2).join();
//
//      assertEquals(1, channel.browse().size());
//      assertEquals(1, channel2.browse().size());
//
//      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING, channel2);
//      assertTrue(channel2.add(receiver));
//
//      log.debug("requesting message");
//      receiver.requestMessages();
//
//      assertTrue(channel.browse().isEmpty());
//      assertTrue(channel2.browse().isEmpty());
//
//      List messages = receiver.getMessages();
//      assertEquals(1, messages.size());
//      Message sm = (Message)messages.iterator().next();
//      assertFalse(sm.isReliable());
//      assertEquals("message0", sm.getMessageID());
//
//      log.info("ok");
//   }

   //
   // Recoverable channel
   //

   ////
   //// Non-reliable message
   ////

   // TODO: Stopped working on this while suspecting a JGroups deadlock due to nested synchronous
   //       remote calls. Uncomment and make the test pass when starting to work on the distributed
   //       part again

//   public void testForwarding_2() throws Exception
//   {
//      if (!channel.isRecoverable())
//      {
//         // we test only recoverable channels now
//         return;
//      }
//
//      jchannel2.connect("testGroup");
//
//      // allow the group time to form
//      Thread.sleep(1000);
//
//      assertTrue(jchannel.isConnected());
//      assertTrue(jchannel2.isConnected());
//
//      // make sure both jchannels joined the group
//      assertEquals(2, jchannel.getView().getMembers().size());
//      assertEquals(2, jchannel2.getView().getMembers().size());
//
//      ((Distributed)channel).join();
//
//      // send a message that will be stored locally
//      Message m = MessageFactory.createCoreMessage("message0", false, "payload");
//      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
//
//      // non-recoverable channel, non-reliable message
//      log.debug("sending message");
//      Delivery delivery = channel.handle(observer, m, null);
//      log.debug("message sent");
//
//      assertTrue(delivery.isDone());
//
//      assertEquals(1, channel.browse().size());
//
//      // the second peer joins
//
//      ((Distributed)channel2).join();
//
//      assertEquals(1, channel.browse().size());
//      assertEquals(1, channel2.browse().size());
//
//      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING, channel2);
//      assertTrue(channel2.add(receiver));
//
//      log.debug("requesting message");
//      receiver.requestMessages();
//
//      assertTrue(channel.browse().isEmpty());
//      assertTrue(channel2.browse().isEmpty());
//
//      List messages = receiver.getMessages();
//      assertEquals(1, messages.size());
//      Message sm = (Message)messages.iterator().next();
//      assertFalse(sm.isReliable());
//      assertEquals("message0", sm.getMessageID());
//
//      log.info("ok");
//   }

   ////
   //// Reliable message
   ////

   // TODO: Stopped working on this while suspecting a JGroups deadlock due to nested synchronous
   //       remote calls. Uncomment and make the test pass when starting to work on the distributed
   //       part again

//   public void testForwarding_3() throws Exception
//   {
//      if (!channel.isRecoverable())
//      {
//         // we test only recoverable channels now
//         return;
//      }
//
//      jchannel2.connect("testGroup");
//
//      // allow the group time to form
//      Thread.sleep(1000);
//
//      assertTrue(jchannel.isConnected());
//      assertTrue(jchannel2.isConnected());
//
//      // make sure both jchannels joined the group
//      assertEquals(2, jchannel.getView().getMembers().size());
//      assertEquals(2, jchannel2.getView().getMembers().size());
//
//      ((Distributed)channel).join();
//
//      // send a message that will be stored locally
//      Message m = MessageFactory.createCoreMessage("message0", true, "payload");
//      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
//
//      // non-recoverable channel, non-reliable message
//      log.debug("sending message");
//      Delivery delivery = channel.handle(observer, m, null);
//      log.debug("message sent");
//
//      assertTrue(delivery.isDone());
//
//      assertEquals(1, channel.browse().size());
//
//      // the second peer joins
//
//      ((Distributed)channel2).join();
//
//      assertEquals(1, channel.browse().size());
//      assertEquals(1, channel2.browse().size());
//
//      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING, channel2);
//      assertTrue(channel2.add(receiver));
//
//      log.debug("requesting message");
//      receiver.requestMessages();
//
//      assertTrue(channel.browse().isEmpty());
//      assertTrue(channel2.browse().isEmpty());
//
//      List messages = receiver.getMessages();
//      assertEquals(1, messages.size());
//      Message sm = (Message)messages.iterator().next();
//      assertFalse(sm.isReliable());
//      assertEquals("message0", sm.getMessageID());
//
//      log.info("ok");
//   }

   //
   //
   //

   /**
    * Tests that sends a Filter between address spaces.
    */
   public void testRemoteBrowse() throws Exception
   {
      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      ((Distributed)channel).join();
      ((Distributed)channel2).join();

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());
      assertFalse(channel2.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message red = MessageFactory.createCoreMessage("message0", false, "payload0");
      red.putHeader("color", "red");
      Message green = MessageFactory.createCoreMessage("message1", false, "payload1");
      green.putHeader("color", "green");

      // non-transacted send, non-reliable message, one message
      assertTrue(channel.handle(observer, red, null).isDone());
      assertTrue(channel.handle(observer, green, null).isDone());

      Filter redFilter = new SimpleFilter("color", "red");

      List messages = channel2.browse(redFilter);
      assertEquals(1, messages.size());
      Message m = (Message)messages.get(0);
      assertEquals("message0", m.getMessageID());
      assertEquals("payload0", m.getPayload());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

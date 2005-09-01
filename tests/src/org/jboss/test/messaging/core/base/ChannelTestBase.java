/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.base;

import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.message.Factory;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;

import java.util.List;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public abstract class ChannelTestBase extends NoTestsChannelTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ChannelTestBase(String name)
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

   // Distributor tests ---------------------------------------------

   public void testAddOneReceiver()
   {
      Receiver r = new SimpleReceiver("ONE");

      assertTrue(channel.add(r));
      assertFalse(channel.add(r));

      assertTrue(channel.contains(r));

      Iterator i = channel.iterator();
      assertEquals(r, i.next());
      assertFalse(i.hasNext());

      channel.clear();
      assertFalse(channel.iterator().hasNext());
   }

   public void testRemoveInexistentReceiver()
   {
      assertFalse(channel.remove(new SimpleReceiver("INEXISTENT")));
   }


   // Channel tests -------------------------------------------------


   public void testClosedChannel() throws Exception
   {
      channel.close();
      try
      {
         channel.handle(null, Factory.createMessage("message0"));
         fail("should throw exception");
      }
      catch(IllegalStateException e)
      {
         //OK
      }
   }

   public void testHandleNullRoutable() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      assertNull(channel.handle(observer, null));
   }

   public void testUnreliableSynchronousDeliveryOneReceiver() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Delivery d = channel.handle(observer, Factory.createMessage("message0", false, "payload"));

      assertTrue(d.isDone());
      List l = r.getMessages();
      assertEquals(1, l.size());
      Message m = (Message)l.get(0);
      assertEquals("payload", m.getPayload());
   }

   public void testReliableSynchronousDeliveryOneReceiver() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Delivery d = channel.handle(observer, Factory.createMessage("message0", true, "payload"));

      assertTrue(d.isDone());
      List l = r.getMessages();
      assertEquals(1, l.size());
      Message m = (Message)l.get(0);
      assertEquals("payload", m.getPayload());
   }


   public void testUnreliableDeliveryNackingReceiver() throws Throwable
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver receiver = new SimpleReceiver("ONE", SimpleReceiver.NACKING);
      assertTrue(channel.add(receiver));

      Routable m = Factory.createMessage("message0", false, "payload");
      Delivery d = channel.handle(observer, m);

      assertTrue(d.isDone());

      List l = receiver.getMessages();
      assertEquals(1, l.size());
      Message rm  = (Message)l.get(0);
      assertEquals("payload", rm.getPayload());

      l = channel.browse();
      assertEquals(1, l.size());
      rm  = (Message)l.get(0);
      assertTrue(rm == m);

      receiver.acknowledge(m);

      l = channel.browse();
      assertTrue(l.isEmpty());

   }

   /**
    * unreliable channel, reliable message, no receiver
    */
   public void testUnreliableChannelReliableMessageNoReceiver() throws Throwable
   {
      if (channel.isReliable())
      {
         return;
      }

      assertNull(channel.handle(null, Factory.createMessage("message0", true, "payload")));
   }

   /**
    * unreliable channel, reliable message, nacking receiver
    */
   public void testUnreliableChannelReliableMessageNACKingReceiver() throws Throwable
   {
      if (channel.isReliable())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver receiver = new SimpleReceiver("ONE", SimpleReceiver.NACKING);
      channel.add(receiver);

      Routable m = Factory.createMessage("message0", true, "payload");
      Delivery d = channel.handle(observer, m);

      assertFalse(d.isDone());

      List l = receiver.getMessages();
      assertEquals(1, l.size());
      assertTrue(m == l.get(0));

      l = channel.browse();
      assertTrue(l.isEmpty());

      receiver.acknowledge(m);

      l = channel.browse();
      assertTrue(l.isEmpty());

   }

   /**
    * reliable channel, reliable message, no receiver
    */
   public void testReliableChannelReliableMessageNoReceiver() throws Throwable
   {
      if (!channel.isReliable())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Routable m = channel.getMessageStore().reference(Factory.createMessage("m0", true, "payload"));

      Delivery d = channel.handle(observer, m);

      assertTrue(d.isDone());
   }


   /**
    * reliable channel, reliable message, nacking receiver
    */
   public void testReliableChannelReliableMessageNACKingReceiver() throws Throwable
   {
      if (!channel.isReliable())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.NACKING);
      channel.add(r);

      Routable m = channel.getMessageStore().reference(Factory.createMessage("m0", true, "payload"));

      Delivery d = channel.handle(observer, m);

      assertTrue(d.isDone());

      List l = r.getMessages();
      assertEquals(1, l.size());
      MessageReference rm  = (MessageReference)l.get(0);
      assertEquals(rm.getMessageID(), m.getMessageID());

      l = channel.browse();
      assertEquals(1, l.size());
      rm  = (MessageReference)l.get(0);
      assertEquals(rm.getMessageID(), m.getMessageID());

      r.acknowledge(m);

      l = channel.browse();
      assertTrue(l.isEmpty());
   }


   public void testReliableChannelFailure() throws Throwable
   {
      if (!channel.isReliable())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.NACKING);
      channel.add(r);

      Routable m = channel.getMessageStore().reference(Factory.createMessage("m0", true, "payload"));
      Delivery d = channel.handle(observer, m);
      assertTrue(d.isDone());

      List l = r.getMessages();
      assertEquals(1, l.size());
      MessageReference rm  = (MessageReference)l.get(0);
      assertEquals(rm.getMessageID(), m.getMessageID());

      crashChannel();

      recoverChannel();

      // make sure the recovered channel still holds the message

      l = channel.browse();
      assertEquals(1, l.size());
      rm  = (MessageReference)l.get(0);
      assertEquals(rm.getMessageID(), m.getMessageID());


      // TODO review this
      try
      {
         r.acknowledge(m);
         fail("should throw exception");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }


   public void testDeliverUnreliableMessageNoReceiver() throws Throwable
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Routable m = channel.getMessageStore().reference(Factory.createMessage("m0", false, "payload"));

      Delivery d = channel.handle(observer, m);
      assertTrue(d.isDone());

      List l = channel.browse();
      assertEquals(1, l.size());
      assertEquals(m.getMessageID(), ((MessageReference)l.get(0)).getMessageID());

      channel.deliver();

      l = channel.browse();
      assertEquals(1, l.size());
      assertEquals(m.getMessageID(), ((MessageReference)l.get(0)).getMessageID());
   }

   public void testDeliverReliableMessageNoReceiver() throws Throwable
   {
      if (!channel.isReliable())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Routable m = channel.getMessageStore().reference(Factory.createMessage("m0", true, "payload"));

      Delivery d = channel.handle(observer, m);

      assertTrue(d.isDone());

      List l = channel.browse();
      assertEquals(1, l.size());
      assertEquals(m.getMessageID(), ((MessageReference)l.get(0)).getMessageID());

      channel.deliver();

      l = channel.browse();
      assertEquals(1, l.size());
      assertEquals(m.getMessageID(), ((MessageReference)l.get(0)).getMessageID());
   }


   public void testDeliverUnreliableMessage() throws Throwable
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Routable m = channel.getMessageStore().reference(Factory.createMessage("m0", false, "payload"));
      Delivery d = channel.handle(observer, m);
      assertTrue(d.isDone());

      List l = channel.browse();
      assertEquals(1, l.size());
      assertEquals(m.getMessageID(), ((MessageReference)l.get(0)).getMessageID());

      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      channel.add(r);

      l = r.getMessages();
      assertEquals(1, l.size());
      assertEquals(m.getMessageID(), ((MessageReference)l.get(0)).getMessageID());

      assertTrue(channel.browse().isEmpty());
   }

   public void testDeliverReliableMessage() throws Throwable
   {

      if (!channel.isReliable())
      {
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Routable m = channel.getMessageStore().reference(Factory.createMessage("m0", true, "payload"));

      Delivery d = channel.handle(observer, m);

      assertTrue(d.isDone());

      List l = channel.browse();
      assertEquals(1, l.size());
      assertEquals(m.getMessageID(), ((MessageReference)l.get(0)).getMessageID());

      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      channel.add(r);

      l = r.getMessages();
      assertEquals(1, l.size());
      assertEquals(m.getMessageID(), ((MessageReference)l.get(0)).getMessageID());

      assertTrue(channel.browse().isEmpty());
   }





   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract void crashChannel() throws Exception;

   protected abstract void recoverChannel() throws Exception;

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}

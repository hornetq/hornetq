/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.local.base;

import org.jboss.test.messaging.core.base.TransactionalChannelTestBase;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.message.Factory;

import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public abstract class QueueTestBase extends TransactionalChannelTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public QueueTestBase(String name)
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

   public void testUnreliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      channel.add(r1);
      channel.add(r2);

      Delivery d = channel.handle(observer, Factory.createMessage("message0", false, "payload"), null);


      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();
      if (l2.isEmpty())
      {
         assertEquals(1, l1.size());
         Message m = (Message)l1.get(0);
         assertEquals("payload", m.getPayload());
      }
      else
      {
         assertTrue(l1.isEmpty());
         assertEquals(1, l2.size());
         Message m = (Message)l2.get(0);
         assertEquals("payload", m.getPayload());
      }
   }


   public void testReliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      assertTrue(channel.add(r1));
      assertTrue(channel.add(r2));

      Delivery d = channel.handle(observer, Factory.createMessage("message0", true, "payload"), null);

      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();
      if (l2.isEmpty())
      {
         assertEquals(1, l1.size());
         Message m = (Message)l1.get(0);
         assertEquals("payload", m.getPayload());
      }
      else
      {
         assertTrue(l1.isEmpty());
         assertEquals(1, l2.size());
         Message m = (Message)l2.get(0);
         assertEquals("payload", m.getPayload());
      }
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

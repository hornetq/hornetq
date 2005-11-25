/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.distributed.replicator.base;

import org.jboss.test.messaging.core.base.DeliveryTestBase;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.messaging.core.distributed.replicator.MultipleReceiversDelivery;
import org.jboss.messaging.core.distributed.replicator.Acknowledgment;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.message.InMemoryMessageStore;
import org.jboss.messaging.core.message.MessageFactory;

import java.io.Serializable;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class MultipleReceiversDeliveryTestBase extends DeliveryTestBase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_RECEIVERS = 10;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected SimpleDeliveryObserver observer;
   protected MessageStore ms;
   protected MessageReference ref;

   // Constructors --------------------------------------------------

   public MultipleReceiversDeliveryTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ms = new InMemoryMessageStore("in-memory-store0");
      ref = ms.reference(MessageFactory.createMessage("message0"));
      observer = new SimpleDeliveryObserver();
   }

   public void tearDown() throws Exception
   {
      observer = null;
      ms = null;
      ref = null;
      super.tearDown();
   }

   public void testAdd()
   {
      Object receiver = createReceiver("group0", "peer0");
      ((MultipleReceiversDelivery)delivery).add(receiver);
      assertEqualsReceiver("peer0", ((MultipleReceiversDelivery)delivery).iterator().next());
   }

   public void testRemove()
   {
      Object receiver = createReceiver("group0", "peer0");
      ((MultipleReceiversDelivery)delivery).add(receiver);
      assertTrue(((MultipleReceiversDelivery)delivery).remove(receiver));
      assertFalse(((MultipleReceiversDelivery)delivery).iterator().hasNext());
   }

   //
   // Delivery that gets cancelled on message rejection
   //

   ////
   //// One receiver
   ////

   public void testDelivery_1() throws Throwable
   {
      if (!((MultipleReceiversDelivery)delivery).cancelOnMessageRejection())
      {
         // we only test deliveries that get cancelled on message rejection
         return;
      }

      Object receiver = createReceiver("group0", "peer0");
      ((MultipleReceiversDelivery)delivery).add(receiver);

      Acknowledgment ack = new Acknowledgment("peer0", ref.getMessageID(), false);
      ((MultipleReceiversDelivery)delivery).handle(ack);

      log.info("acknowledgment handled");

      observer.waitForCancellation(delivery);
      assertTrue(delivery.isCancelled());
      assertFalse(delivery.isDone());
   }

   ////
   //// Multiple receivers
   ////

   public void testDelivery_2() throws Throwable
   {
      if (!((MultipleReceiversDelivery)delivery).cancelOnMessageRejection())
      {
         // we only test deliveries that get cancelled on message rejection
         return;
      }

      for(int i = 0; i < NUMBER_OF_RECEIVERS; i++)
      {
         Object receiver = createReceiver("group0", "peer" + i);
         ((MultipleReceiversDelivery)delivery).add(receiver);
      }

      Acknowledgment ack = new Acknowledgment("peer5", ref.getMessageID(), false);
      ((MultipleReceiversDelivery)delivery).handle(ack);

      observer.waitForCancellation(delivery);
      assertTrue(delivery.isCancelled());
      assertFalse(delivery.isDone());
   }

   //
   // Delivery that DOES NOT get cancelled on message rejection
   //

   ////
   //// One receiver
   ////

   public void testDelivery_3() throws Throwable
   {
      if (((MultipleReceiversDelivery)delivery).cancelOnMessageRejection())
      {
         // we only test deliveries that DO NOT get cancelled on message rejection
         return;
      }

      Object receiver = createReceiver("group0", "peer0");
      ((MultipleReceiversDelivery)delivery).add(receiver);

      Acknowledgment ack = new Acknowledgment("peer0", ref.getMessageID(), true);
      ((MultipleReceiversDelivery)delivery).handle(ack);

      log.info("acknowledgment handled");

      observer.waitForAcknowledgment(delivery);
      assertTrue(delivery.isDone());
      assertFalse(delivery.isCancelled());
   }

   ////
   //// Multiple receivers
   ////

   public void testDelivery_4() throws Throwable
   {
      if (((MultipleReceiversDelivery)delivery).cancelOnMessageRejection())
      {
         // we only test deliveries that get cancelled on message rejection
         return;
      }

      for(int i = 0; i < NUMBER_OF_RECEIVERS; i++)
      {
         Object receiver = createReceiver("group0", "peer" + i);
         ((MultipleReceiversDelivery)delivery).add(receiver);
      }

      for(int i = 0; i < NUMBER_OF_RECEIVERS; i++)
      {
         Acknowledgment ack = new Acknowledgment("peer" + i, ref.getMessageID(), false);
         ((MultipleReceiversDelivery)delivery).handle(ack);


         if (i < NUMBER_OF_RECEIVERS - 1)
         {
            assertFalse(observer.waitForAcknowledgment(delivery, 10));
            assertFalse(delivery.isDone());
            assertFalse(delivery.isCancelled());
         }
         else
         {
            // the last receiver
            assertTrue(observer.waitForAcknowledgment(delivery));
            assertTrue(delivery.isDone());
            assertFalse(delivery.isCancelled());
         }
      }
   }

   // Package protected ---------------------------------------------

   protected abstract Object createReceiver(Serializable replicatorID, Serializable outputID);
   protected abstract void assertEqualsReceiver(Serializable outputID, Object receiver);


   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}

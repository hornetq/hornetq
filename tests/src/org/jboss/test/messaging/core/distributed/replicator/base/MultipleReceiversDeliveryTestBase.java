/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.distributed.replicator.base;

import java.io.Serializable;

import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.distributed.replicator.Acknowledgment;
import org.jboss.messaging.core.distributed.replicator.MultipleReceiversDelivery;
import org.jboss.messaging.core.plugin.InMemoryMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.base.DeliveryTestBase;
import org.jboss.messaging.core.message.MessageFactory;


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
      ref = ms.reference(MessageFactory.createCoreMessage("message0"));
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

      Acknowledgment ack = new Acknowledgment("peer0", ref.getMessageID(), Acknowledgment.REJECTED);
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

      Acknowledgment ack = new Acknowledgment("peer5", ref.getMessageID(), Acknowledgment.REJECTED);
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

      Acknowledgment ack = new Acknowledgment("peer0", ref.getMessageID(), Acknowledgment.ACCEPTED);
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
         Acknowledgment ack = new Acknowledgment("peer" + i, ref.getMessageID(),
                                                 Acknowledgment.REJECTED);
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


   //
   // Acknowledgment.ACCEPTED
   //

   public void testDelivery_5() throws Throwable
   {
      for(int i = 0; i < NUMBER_OF_RECEIVERS; i++)
      {
         Object receiver = createReceiver("group0", "peer" + i);
         ((MultipleReceiversDelivery)delivery).add(receiver);
      }

      for(int i = 0; i < NUMBER_OF_RECEIVERS; i++)
      {
         Acknowledgment ack = new Acknowledgment("peer" + i, ref.getMessageID(),
                                                 Acknowledgment.ACCEPTED);
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

   /**
    * Three receivers that ACCEPT, REJECT and CANCEL.
    */
   public void testMixedAcknowledgments() throws Throwable
   {
      if (((MultipleReceiversDelivery)delivery).cancelOnMessageRejection())
      {
         // we only test deliveries that DON'T get cancelled on message rejection
         return;
      }

      Object receiver = null;

      receiver = createReceiver("group0", "ACKING");
      ((MultipleReceiversDelivery)delivery).add(receiver);
      receiver = createReceiver("group0", "REJECTING");
      ((MultipleReceiversDelivery)delivery).add(receiver);
      receiver = createReceiver("group0", "CANCELLING");
      ((MultipleReceiversDelivery)delivery).add(receiver);

      Acknowledgment ack = null;

      ack = new Acknowledgment("ACKING", ref.getMessageID(), Acknowledgment.ACCEPTED);
      ((MultipleReceiversDelivery)delivery).handle(ack);

      assertFalse(delivery.isCancelled());
      assertFalse(delivery.isDone());

      ack = new Acknowledgment("REJECTING", ref.getMessageID(), Acknowledgment.REJECTED);
      ((MultipleReceiversDelivery)delivery).handle(ack);

      assertFalse(delivery.isCancelled());
      assertFalse(delivery.isDone());

      ack = new Acknowledgment("CANCELLING", ref.getMessageID(), Acknowledgment.CANCELLED);
      ((MultipleReceiversDelivery)delivery).handle(ack);

      // TODO
      // This is a problem: 'only once' guarantee cannot be kept here: "ACKING" and possibly
      // "REJECTING" (if it stops rejecting) will receive the message twice.

      assertTrue(delivery.isCancelled());
      assertFalse(delivery.isDone());

   }




   // Package protected ---------------------------------------------

   protected abstract Object createReceiver(Serializable replicatorID, Serializable outputID);
   protected abstract void assertEqualsReceiver(Serializable outputID, Object receiver);


   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}

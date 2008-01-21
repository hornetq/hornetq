/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat.test.unit;

import java.util.List;

import junit.framework.Assert;

import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.delegate.Ack;
import org.jboss.jms.delegate.Cancel;
import org.jboss.jms.delegate.DeliveryRecovery;
import org.jboss.messaging.core.tx.MessagingXid;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class CodecAssert extends Assert
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static void assertEqualsAcks(List<Ack> expected, List<Ack> actual)
   {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++)
      {
         assertEquals(expected.get(i).getDeliveryID(), actual.get(i)
               .getDeliveryID());
      }
   }

   static void assertEqualsDeliveries(List<DeliveryRecovery> expected,
         List<DeliveryRecovery> actual)
   {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++)
      {
         DeliveryRecovery expectedDelivery = expected.get(i);
         DeliveryRecovery actualDelivery = actual.get(i);
         assertEquals(expectedDelivery.getDeliveryID(), actualDelivery
               .getDeliveryID());
         assertEquals(expectedDelivery.getMessageID(), actualDelivery
               .getMessageID());
         assertEquals(expectedDelivery.getQueueName(), actualDelivery
               .getQueueName());
      }
   }

   static void assertEqualsCancels(List<Cancel> expected, List<Cancel> actual)
   {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++)
      {
         Cancel expectedCancel = expected.get(i);
         Cancel actualCancel = actual.get(i);
         assertEquals(expectedCancel.getDeliveryId(), actualCancel
               .getDeliveryId());
         assertEquals(expectedCancel.getDeliveryCount(), actualCancel
               .getDeliveryCount());
         assertEquals(expectedCancel.isExpired(), actualCancel.isExpired());
         assertEquals(expectedCancel.isReachedMaxDeliveryAttempts(),
               actualCancel.isReachedMaxDeliveryAttempts());
      }
   }

   static void assertSameXids(MessagingXid[] expected, MessagingXid[] actual)
   {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         MessagingXid expectedXid = expected[i];
         MessagingXid actualXid = actual[i];
         assertEqualsByteArrays(expectedXid.getBranchQualifier(), actualXid
               .getBranchQualifier());
         assertEquals(expectedXid.getFormatId(), actualXid.getFormatId());
         assertEqualsByteArrays(expectedXid.getGlobalTransactionId(), actualXid
               .getGlobalTransactionId());
      }
   }

   static void assertEqualsByteArrays(byte[] expected, byte[] actual)
   {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   static void assertEqualsByteArrays(int length, byte[] expected, byte[] actual)
   {
      // we check only for the given length (the arrays might be
      // larger)
      assertTrue(expected.length >= length);
      assertTrue(actual.length >= length);
      for (int i = 0; i < length; i++)
      {
         assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

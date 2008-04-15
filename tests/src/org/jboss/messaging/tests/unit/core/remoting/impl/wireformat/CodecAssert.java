/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting.impl.wireformat;

import junit.framework.Assert;

import org.jboss.messaging.core.transaction.impl.XidImpl;

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

   static void assertSameXids(XidImpl[] expected, XidImpl[] actual)
   {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         XidImpl expectedXid = expected[i];
         XidImpl actualXid = actual[i];
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

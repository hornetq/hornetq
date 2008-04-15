/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.util;

import static java.util.UUID.randomUUID;

import java.util.Random;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class RandomUtil
{
   // Constants -----------------------------------------------------

   private static final Random random = new Random(System.currentTimeMillis());

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String randomString()
   {
      return randomUUID().toString();
   }

   public static long randomLong()
   {
      return random.nextLong();
   }

   public static int randomInt()
   {
      return random.nextInt();
   }

   public static byte randomByte()
   {
      return Integer.valueOf(random.nextInt()).byteValue();
   }

   public static byte[] randomBytes()
   {
      return randomString().getBytes();
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

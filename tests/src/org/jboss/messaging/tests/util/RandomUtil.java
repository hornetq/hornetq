/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.util;

import static java.util.UUID.randomUUID;

import java.util.Random;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.util.SimpleString;

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
   
   public static SimpleString randomSimpleString()
   {
      return new SimpleString(randomString());
   }
   
   public static char randomChar()
   {
      return randomString().charAt(0);
   }

   public static long randomLong()
   {
      return random.nextLong();
   }

   public static int randomInt()
   {
      return random.nextInt();
   }
   
   public static short randomShort()
   {
      return (short) random.nextInt(Short.MAX_VALUE);
   }

   public static byte randomByte()
   {
      return Integer.valueOf(random.nextInt()).byteValue();
   }
   
   public static boolean randomBoolean()
   {
      return random.nextBoolean();
   }

   public static byte[] randomBytes()
   {
      return randomString().getBytes();
   }
   
   public static double randomDouble()
   {
      return random.nextDouble();
   }
   
   public static float randomFloat()
   {
      return random.nextFloat();
   }
   
   public static Xid randomXid()
   {      
      return new XidImpl(randomBytes(), randomInt(), randomBytes());
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

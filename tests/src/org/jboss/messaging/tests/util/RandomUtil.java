/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

   public static long randomPositiveLong()
   {
      return Math.abs(randomLong());
   }

   public static int randomInt()
   {
      return random.nextInt();
   }
   
   public static int randomPositiveInt()
   {
      return Math.abs(randomInt());
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
   
   public static byte[] randomBytes(int length)
   {
      byte[] bytes = new byte[length];
      for (int i = 0; i < bytes.length; i++)
      {
        bytes[i] = randomByte();
      }
      return bytes;
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

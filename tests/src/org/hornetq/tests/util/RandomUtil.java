/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */ 

package org.hornetq.tests.util;

import static java.util.UUID.randomUUID;

import java.util.Random;

import javax.transaction.xa.Xid;

import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.utils.SimpleString;

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
   
   public static int randomPort()
   {
      return random.nextInt(65536);
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

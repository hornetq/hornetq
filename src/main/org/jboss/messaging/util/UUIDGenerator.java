/* JUG Java Uuid Generator
 *
 * Copyright (c) 2002- Tatu Saloranta, tatu.saloranta@iki.fi
 *
 * Licensed under the License specified in the file licenses/LICENSE.txt which is
 * included with the source code.
 * You may not use this file except in compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jboss.messaging.util;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.Random;

import org.jboss.messaging.core.logging.Logger;

public final class UUIDGenerator
{
   private final static UUIDGenerator sSingleton = new UUIDGenerator();

   private static final Logger log = Logger.getLogger(UUIDGenerator.class);

   /**
    * Random-generator, used by various UUID-generation methods:
    */
   private Random mRnd = null;

   private final Object mTimerLock = new Object();
   private UUIDTimer mTimer = null;
   private byte[] address;
   

   /**
    * Constructor is private to enforce singleton access.
    */
   private UUIDGenerator()
   {
   }

   /**
    * Method used for accessing the singleton generator instance.
    */
   public static UUIDGenerator getInstance()
   {
      return sSingleton;
   }

   /*
    * ///////////////////////////////////////////////////// // Configuration
    * /////////////////////////////////////////////////////
    */

   /**
    * Method for getting the shared random number generator used for generating
    * the UUIDs. This way the initialization cost is only taken once; access
    * need not be synchronized (or in cases where it has to, SecureRandom takes
    * care of it); it might even be good for getting really 'random' stuff to
    * get shared access...
    */
   public final Random getRandomNumberGenerator()
   {
      /*
       * Could be synchronized, but since side effects are trivial (ie.
       * possibility of generating more than one SecureRandom, of which all but
       * one are dumped) let's not add synchronization overhead:
       */
      if (mRnd == null)
      {
         mRnd = new SecureRandom();
      }
      return mRnd;
   }

   public final UUID generateTimeBasedUUID(byte[] byteAddr)
   {
      byte[] contents = new byte[16];
      int pos = 12;
      for (int i = 0; i < 4; ++i)
      {
         contents[pos + i] = byteAddr[i];
      }

      synchronized (mTimerLock)
      {
         if (mTimer == null)
         {
            mTimer = new UUIDTimer(getRandomNumberGenerator());
         }

         mTimer.getTimestamp(contents);
      }

      return new UUID(UUID.TYPE_TIME_BASED, contents);
   }
   
   public final byte[] generateDummyAddress()
   {
      Random rnd = getRandomNumberGenerator();
      byte[] dummy = new byte[6];
      rnd.nextBytes(dummy);
      /* Need to set the broadcast bit to indicate it's not a real
       * address.
       */
      dummy[0] |= (byte) 0x01;
      
      if (log.isDebugEnabled())
      {
         log.debug("using dummy address " + asString(dummy));
      }
      return dummy;
   }

   /**
    * If running java 6 or above, returns {@link NetworkInterface#getHardwareAddress()}, else return <code>null</code>.
    * The first hardware address is returned when iterating all the NetworkInterfaces
    */
   public final static byte[] getHardwareAddress()
   {
      Method getHardwareAddressMethod;
      try
      {
         getHardwareAddressMethod = NetworkInterface.class.getMethod("getHardwareAddress", null);
      }
      catch (Throwable t)
      {
         // not on Java 6 or not enough security permission
         return null;
      }
      
      try {
         Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
         while (networkInterfaces.hasMoreElements())
         {
            NetworkInterface networkInterface = (NetworkInterface)networkInterfaces.nextElement();
            Object res = getHardwareAddressMethod.invoke(networkInterface, null);
            if (res instanceof byte[])
            {
               byte[] address = (byte[])res;
               if (log.isDebugEnabled())
               {
                  log.debug("using hardware address " + asString(address));
               }
               return address;
            }
         }
      } catch (Throwable t)
      {
      }

      return null;
   }
   
   /**
    * Browse all the network interfaces and their addresses until we find the 1st InetAddress which
    * is neither a loopback address nor a site local address.
    * Returns <code>null</code> if no such address is found.
    */
   public final static InetAddress getInetAddress()
   {
         try
         {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements())
            {
               NetworkInterface networkInterface = (NetworkInterface)networkInterfaces.nextElement();
               Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
               while (inetAddresses.hasMoreElements())
               {
                  InetAddress inetAddress = (InetAddress)inetAddresses.nextElement();
                  if (!inetAddress.isLoopbackAddress()
                      && !inetAddress.isSiteLocalAddress())
                  {
                     if (log.isDebugEnabled())
                     {
                        log.debug("using inet address " + inetAddress);
                     }
                     return inetAddress;
                  }
               }
            }
         }
         catch (SocketException e)
         {
         }
         return null;
   }
   
   public final SimpleString generateSimpleStringUUID()
   {
      byte[] address = getAddressBytes();
      if (address == null)
      {
         return new SimpleString(java.util.UUID.randomUUID().toString());
      }
      else
      {
         UUIDGenerator gen = UUIDGenerator.getInstance();
         return new SimpleString(gen.generateTimeBasedUUID(address).toString());
      }    
   }
   
   public final UUID generateUUID()
   {
      byte[] address = getAddressBytes();
      
      UUIDGenerator gen = UUIDGenerator.getInstance();
      UUID uid = gen.generateTimeBasedUUID(address);         
      
      return uid;
   }
   
   public final String generateStringUUID()
   {
      byte[] address = getAddressBytes();
      
      if (address == null)
      {
         return java.util.UUID.randomUUID().toString();
      }
      else
      {
         UUIDGenerator gen = UUIDGenerator.getInstance();
         return gen.generateTimeBasedUUID(address).toString();
      }    
   }
   
   // Private -------------------------------------------------------
   
   private final byte[] getAddressBytes()
   {
      if (address == null)
      {
         address = getHardwareAddress();
         if (address == null)
         {
            InetAddress addr = getInetAddress();
            if (addr != null)
            {
               address = addr.getAddress();
            }
         }
         if (address == null)
         {
            address = generateDummyAddress();
         }
      }
      
      return address;
   }
   
   private static final String asString(byte[] bytes)
   {
      if (bytes == null)
      {
         return null;
      }
      
      String s = "";
      for (int i = 0; i < bytes.length - 1; i++)
      {
         s += Integer.toHexString(bytes[i]) + ":";
      }
      s += bytes[bytes.length - 1];
      return s;
   }
}

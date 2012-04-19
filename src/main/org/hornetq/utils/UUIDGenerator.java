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

package org.hornetq.utils;

import java.lang.reflect.Method;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;

public final class UUIDGenerator
{
   private final static UUIDGenerator sSingleton = new UUIDGenerator();

   private static final Logger log = Logger.getLogger(UUIDGenerator.class);

   // Windows has some fake adapters that will return the same HARDWARE ADDRESS on any computer. We need to ignore those
   private static final byte[][] BLACK_LIST = new byte[][] { { 2, 0, 84, 85, 78, 1 } };

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
      return UUIDGenerator.sSingleton;
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

   public final UUID generateTimeBasedUUID(final byte[] byteAddr)
   {
      byte[] contents = new byte[16];
      int pos = 10;
      for (int i = 0; i < 6; ++i)
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
      dummy[0] |= (byte)0x01;

      if (UUIDGenerator.log.isDebugEnabled())
      {
         UUIDGenerator.log.debug("using dummy address " + UUIDGenerator.asString(dummy));
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
      Method isUpMethod;
      Method isLoopbackMethod;
      Method isVirtualMethod;
      try
      {
         getHardwareAddressMethod = NetworkInterface.class.getMethod("getHardwareAddress");
         isUpMethod = NetworkInterface.class.getMethod("isUp");
         isLoopbackMethod = NetworkInterface.class.getMethod("isLoopback");
         isVirtualMethod = NetworkInterface.class.getMethod("isVirtual");
         // check if we have enough security permissions to create and shutdown an executor
         ExecutorService executor = Executors.newFixedThreadPool(0);
         executor.shutdownNow();
      }
      catch (Throwable t)
      {
         // not on Java 6 or not enough security permission
         return null;
      }

      try
      {
         List<NetworkInterface> ifaces = getAllNetworkInterfaces();

         if (ifaces.size() == 0) {
            return null;
         }

         byte[] address = findFirstMatchingHardwareAddress(ifaces,
                                              getHardwareAddressMethod,
                                              isUpMethod,
                                              isLoopbackMethod,
                                              isVirtualMethod);
         if (address != null) {
            if (UUIDGenerator.log.isDebugEnabled())
            {
               UUIDGenerator.log.debug("using hardware address " + UUIDGenerator.asString(address));
            }
            return address;
         }
         return null;
      } catch(Exception e)
      {
         return null;
      }
   }

   public final SimpleString generateSimpleStringUUID()
   {
      return new SimpleString(generateStringUUID());
   }

   public final UUID generateUUID()
   {
      byte[] address = getAddressBytes();

      UUID uid = generateTimeBasedUUID(address);

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
         return generateTimeBasedUUID(address).toString();
      }
   }

   public final static byte[] getZeroPaddedSixBytes(final byte[] bytes)
   {
      if (bytes == null)
      {
         return null;
      }
      if (bytes.length > 0 && bytes.length <= 6)
      {
         if (bytes.length == 6)
         {
            return bytes;
         }
         else
         {
            // pad with zeroes to have a 6-byte array
            byte[] paddedAddress = new byte[6];
            System.arraycopy(bytes, 0, paddedAddress, 0, bytes.length);
            for (int i = bytes.length; i < 6; i++)
            {
               paddedAddress[i] = 0;
            }
            return paddedAddress;
         }
      }
      return null;
   }

   // Private -------------------------------------------------------

   private static final boolean isBlackList(final byte[] address)
   {
      for (byte[] blackList : UUIDGenerator.BLACK_LIST)
      {
         if (Arrays.equals(address, blackList))
         {
            return true;
         }
      }
      return false;
   }

   private final byte[] getAddressBytes()
   {
      if (address == null)
      {
         // calling UUIDGenerator.getHardwareAddress() is a
         // time-expensive operation, let make sure it is called
         // only once
         synchronized (this)
         {
            if (address == null)
            {
               address = UUIDGenerator.getHardwareAddress();
               if (address == null)
               {
                  address = generateDummyAddress();
               }
            }
         }
      }

      return address;
   }

   private static final String asString(final byte[] bytes)
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

   private static List<NetworkInterface> getAllNetworkInterfaces()
   {
      Enumeration<NetworkInterface> networkInterfaces;
      try
      {
         networkInterfaces = NetworkInterface.getNetworkInterfaces();

         List<NetworkInterface> ifaces = new ArrayList<NetworkInterface>();
         while (networkInterfaces.hasMoreElements())
         {
            ifaces.add(networkInterfaces.nextElement());
         }
         return ifaces;
      }
      catch (SocketException e)
      {
         return Collections.emptyList();
      }
   }

   private static byte[] findFirstMatchingHardwareAddress(List<NetworkInterface> ifaces,
                                                          final Method getHardwareAddressMethod,
                                                          final Method isUpMethod,
                                                          final Method isLoopbackMethod,
                                                          final Method isVirtualMethod)
   {
      ExecutorService executor = Executors.newFixedThreadPool(ifaces.size());
      Collection<Callable<byte[]>> tasks = new ArrayList<Callable<byte[]>>();

      for (final NetworkInterface networkInterface : ifaces)
      {
         tasks.add(new Callable<byte[]>()
                   {
            public byte[] call() throws Exception
            {
               boolean up = (Boolean)isUpMethod.invoke(networkInterface );
               boolean loopback = (Boolean)isLoopbackMethod.invoke(networkInterface);
               boolean virtual = (Boolean)isVirtualMethod.invoke(networkInterface);

               if (loopback || virtual || !up)
               {
                  throw new Exception("not suitable interface");
               }

               Object res = getHardwareAddressMethod.invoke(networkInterface);
               if (res != null && res instanceof byte[]) {

                  byte[] address = (byte[])res;
                  byte[] paddedAddress = UUIDGenerator.getZeroPaddedSixBytes(address);

                  if (UUIDGenerator.isBlackList(address))
                  {
                     throw new Exception("black listed address");
                  }

                  if (paddedAddress != null)
                  {
                     return paddedAddress;
                  }
               }

               throw new Exception("invalid network interface");
            }
         });
      }
      try
      {
         // we wait 5 seconds to get the first matching hardware address. After that, we give up and return null
         byte[] address = executor.invokeAny(tasks, 5, TimeUnit.SECONDS);
         return address;
      }
      catch (Exception e)
      {
         return null;
      }
      finally
      {
         executor.shutdownNow();
      }
   }
}

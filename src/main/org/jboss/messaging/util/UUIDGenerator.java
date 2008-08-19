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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Random;

public final class UUIDGenerator
{
   private final static UUIDGenerator sSingleton = new UUIDGenerator();

   /**
    * Random-generator, used by various UUID-generation methods:
    */
   private Random mRnd = null;

   private final Object mTimerLock = new Object();
   private UUIDTimer mTimer = null;

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
   public Random getRandomNumberGenerator()
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

   public UUID generateTimeBasedUUID(InetAddress addr)
   {
      byte[] contents = new byte[16];
      byte[] byteAddr = addr.getAddress();
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
   
   public SimpleString generateSimpleStringUUID()
   {
      InetAddress localHost = null;
      
      try
      {
         localHost = InetAddress.getLocalHost();
      }
      catch (UnknownHostException e)
      {        
      }
      SimpleString uid;
      if (localHost == null)
      {
         uid = new SimpleString(java.util.UUID.randomUUID().toString());
      }
      else
      {
         UUIDGenerator gen = UUIDGenerator.getInstance();
         uid = new SimpleString(gen.generateTimeBasedUUID(localHost).toString());
      }    
      
      return uid;
   }
}

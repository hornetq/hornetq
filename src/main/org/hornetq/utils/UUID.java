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

/**
 * UUID represents Universally Unique Identifiers (aka Global UID in Windows
 * world). UUIDs are usually generated via UUIDGenerator (or in case of 'Null
 * UUID', 16 zero bytes, via static method getNullUUID()), or received from
 * external systems.
 * 
 * By default class caches the string presentations of UUIDs so that description
 * is only created the first time it's needed. For memory stingy applications
 * this caching can be turned off (note though that if uuid.toString() is never
 * called, desc is never calculated so only loss is the space allocated for the
 * desc pointer... which can of course be commented out to save memory).
 * 
 * Similarly, hash code is calculated when it's needed for the first time, and
 * from thereon that value is just returned. This means that using UUIDs as keys
 * should be reasonably efficient.
 * 
 * UUIDs can be compared for equality, serialized, cloned and even sorted.
 * Equality is a simple bit-wise comparison. Ordering (for sorting) is done by
 * first ordering based on type (in the order of numeric values of types),
 * secondarily by time stamp (only for time-based time stamps), and finally by
 * straight numeric byte-by-byte comparison (from most to least significant
 * bytes).
 */

public final class UUID
{
   private final static String kHexChars = "0123456789abcdefABCDEF";

   public final static byte INDEX_CLOCK_HI = 6;

   public final static byte INDEX_CLOCK_MID = 4;

   public final static byte INDEX_CLOCK_LO = 0;

   public final static byte INDEX_TYPE = 6;

   // Clock seq. & variant are multiplexed...
   public final static byte INDEX_CLOCK_SEQUENCE = 8;

   public final static byte INDEX_VARIATION = 8;

   public final static byte TYPE_NULL = 0;

   public final static byte TYPE_TIME_BASED = 1;

   public final static byte TYPE_DCE = 2; // Not used

   public final static byte TYPE_NAME_BASED = 3;

   public final static byte TYPE_RANDOM_BASED = 4;

   /*
    * 'Standard' namespaces defined (suggested) by UUID specs:
    */
   public final static String NAMESPACE_DNS = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

   public final static String NAMESPACE_URL = "6ba7b811-9dad-11d1-80b4-00c04fd430c8";

   public final static String NAMESPACE_OID = "6ba7b812-9dad-11d1-80b4-00c04fd430c8";

   public final static String NAMESPACE_X500 = "6ba7b814-9dad-11d1-80b4-00c04fd430c8";

   /*
    * By default let's cache desc, can be turned off. For hash code there's no
    * point in turning it off (since the int is already part of the instance
    * memory allocation); if you want to save those 4 bytes (or possibly bit
    * more if alignment is bad) just comment out hash caching.
    */
   private static boolean sDescCaching = true;

   private final byte[] mId;

   // Both string presentation and hash value may be cached...
   private transient String mDesc = null;

   private transient int mHashCode = 0;

   /**
    * 
    * 
    * @param type
    *           UUID type
    * @param data
    *           16 byte UUID contents
    */
   public UUID(final int type, final byte[] data)
   {
      mId = data;
      // Type is multiplexed with time_hi:
      mId[UUID.INDEX_TYPE] &= (byte)0x0F;
      mId[UUID.INDEX_TYPE] |= (byte)(type << 4);
      // Variant masks first two bits of the clock_seq_hi:
      mId[UUID.INDEX_VARIATION] &= (byte)0x3F;
      mId[UUID.INDEX_VARIATION] |= (byte)0x80;
   }

   public final byte[] asBytes()
   {
      return mId;
   }

   /**
    * Could use just the default hash code, but we can probably create a better
    * identity hash (ie. same contents generate same hash) manually, without
    * sacrificing speed too much. Although multiplications with modulos would
    * generate better hashing, let's use just shifts, and do 2 bytes at a time.
    * <p>
    * Of course, assuming UUIDs are randomized enough, even simpler approach
    * might be good enough?
    * <p>
    * Is this a good hash? ... one of these days I better read more about basic
    * hashing techniques I swear!
    */
   private final static int[] kShifts = { 3, 7, 17, 21, 29, 4, 9 };

   @Override
   public final int hashCode()
   {
      if (mHashCode == 0)
      {
         // Let's handle first and last byte separately:
         int result = mId[0] & 0xFF;

         result |= result << 16;
         result |= result << 8;

         for (int i = 1; i < 15; i += 2)
         {
            int curr = (mId[i] & 0xFF) << 8 | mId[i + 1] & 0xFF;
            int shift = UUID.kShifts[i >> 1];

            if (shift > 16)
            {
               result ^= curr << shift | curr >>> 32 - shift;
            }
            else
            {
               result ^= curr << shift;
            }
         }

         // and then the last byte:
         int last = mId[15] & 0xFF;
         result ^= last << 3;
         result ^= last << 13;

         result ^= last << 27;
         // Let's not accept hash 0 as it indicates 'not hashed yet':
         if (result == 0)
         {
            mHashCode = -1;
         }
         else
         {
            mHashCode = result;
         }
      }
      return mHashCode;
   }

   @Override
   public final String toString()
   {
      /*
       * Could be synchronized, but there isn't much harm in just taking our
       * chances (ie. in the worst case we'll form the string more than once...
       * but result is the same)
       */

      if (mDesc == null)
      {
         StringBuffer b = new StringBuffer(36);

         for (int i = 0; i < 16; ++i)
         {
            // Need to bypass hyphens:
            switch (i)
            {
               case 4:
               case 6:
               case 8:
               case 10:
                  b.append('-');
            }
            int hex = mId[i] & 0xFF;
            b.append(UUID.kHexChars.charAt(hex >> 4));
            b.append(UUID.kHexChars.charAt(hex & 0x0f));
         }
         if (!UUID.sDescCaching)
         {
            return b.toString();
         }
         mDesc = b.toString();
      }
      return mDesc;
   }

   /**
    * Checking equality of UUIDs is easy; just compare the 128-bit number.
    */
   @Override
   public final boolean equals(final Object o)
   {
      if (!(o instanceof UUID))
      {
         return false;
      }
      byte[] otherId = ((UUID)o).mId;
      byte[] thisId = mId;
      for (int i = 0; i < 16; ++i)
      {
         if (otherId[i] != thisId[i])
         {
            return false;
         }
      }
      return true;
   }
}

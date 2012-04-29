/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.utils;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

/**
 * A DefaultSensitiveDataCodec
 * 
 * The default implementation of SensitiveDataCodec.
 * This class is used when the user indicates in the config
 * file to use a masked password but doesn't give a 
 * codec implementation. 
 * 
 * The decode() and encode() method is copied originally from
 * JBoss AS code base.
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 *
 */
public class DefaultSensitiveStringCodec implements SensitiveDataCodec<String>
{
   private byte[] KEY_BYTES = "clusterpassword".getBytes();

   public String decode(Object secret) throws NoSuchPaddingException,
                                      NoSuchAlgorithmException,
                                      InvalidKeyException,
                                      BadPaddingException,
                                      IllegalBlockSizeException
   {
      SecretKeySpec key = new SecretKeySpec(KEY_BYTES, "Blowfish");

      BigInteger n = new BigInteger((String)secret, 16);
      byte[] encoding = n.toByteArray();

      // JBAS-3457: fix leading zeros
      if (encoding.length % 8 != 0)
      {
         int length = encoding.length;
         int newLength = ((length / 8) + 1) * 8;
         int pad = newLength - length; // number of leading zeros
         byte[] old = encoding;
         encoding = new byte[newLength];
         for (int i = old.length - 1; i >= 0; i--)
         {
            encoding[i + pad] = old[i];
         }
      }

      Cipher cipher = Cipher.getInstance("Blowfish");
      cipher.init(Cipher.DECRYPT_MODE, key);
      byte[] decode = cipher.doFinal(encoding);

      return new String(decode);
   }

   public Object encode(String secret) throws NoSuchPaddingException,
                                      NoSuchAlgorithmException,
                                      InvalidKeyException,
                                      BadPaddingException,
                                      IllegalBlockSizeException
   {
      SecretKeySpec key = new SecretKeySpec(KEY_BYTES, "Blowfish");

      Cipher cipher = Cipher.getInstance("Blowfish");
      cipher.init(Cipher.ENCRYPT_MODE, key);
      byte[] encoding = cipher.doFinal(secret.getBytes());
      BigInteger n = new BigInteger(encoding);
      return n.toString(16);
   }
   
   public static void main(String[] args) throws Exception
   {
      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      Object encode = codec.encode(args[0]);
      System.out.println("Encoded password: " + encode);
   }

   public void init(Map<String, String> params)
   {
      String key = params.get("key");
      if (key != null)
      {
         updateKey(key);
      }
   }

   private void updateKey(String key)
   {
      KEY_BYTES = key.getBytes();
   }

   
}

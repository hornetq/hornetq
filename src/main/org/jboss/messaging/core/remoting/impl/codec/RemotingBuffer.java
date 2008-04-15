/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import java.nio.charset.CharacterCodingException;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface RemotingBuffer
{
   void put(byte byteValue);

   void putInt(int intValue);

   void putLong(long longValue);

   void put(byte[] bytes);

   void putFloat(float floatValue);

   byte get();

   int remaining();

   int getInt();

   long getLong();

   void get(byte[] b);

   float getFloat();

   void putBoolean(boolean b);

   boolean getBoolean();

   void putNullableString(String nullableString)
         throws CharacterCodingException;

   String getNullableString() throws CharacterCodingException;
   
   void rewind();

}

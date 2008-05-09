/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;


/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class BytesPacket extends EmptyPacket
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private byte[] bytes;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BytesPacket(final byte[] bytes)
   {
      super(BYTES);

      this.bytes = bytes;
   }
   
   public BytesPacket()
   {
      super(BYTES);
   }
   
   // Public --------------------------------------------------------

   public byte[] getBytes()
   {
      return bytes;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", bytes.length=" + bytes.length + "]";
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(bytes.length);
      
      buffer.putBytes(bytes);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      bytes = new byte[buffer.getInt()];
      
      buffer.getBytes(bytes);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------   
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.BYTES;


/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class BytesPacket extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final byte[] bytes;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BytesPacket(byte[] bytes)
   {
      super(BYTES);

      assert bytes != null;

      this.bytes = bytes;
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------   
}

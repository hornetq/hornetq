/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_IDBLOCK;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class IDBlockRequest extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final int size;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public IDBlockRequest(int size)
   {
      super(REQ_IDBLOCK);

      this.size = size;
   }

   // Public --------------------------------------------------------

   public int getSize()
   {
      return size;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", size=" + size + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_IDBLOCK;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class IDBlockResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long low;
   private final long high;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * 
    */
   public IDBlockResponse(long low, long high)
   {
      super(RESP_IDBLOCK);

      this.low = low;
      this.high = high;
   }

   // Public --------------------------------------------------------

   public long getLow()
   {
      return low;
   }

   public long getHigh()
   {
      return high;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", low=" + low + ", high=" + high + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

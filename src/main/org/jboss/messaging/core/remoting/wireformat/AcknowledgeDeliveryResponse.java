/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_ACKDELIVERY;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgeDeliveryResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final boolean acknowledged;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcknowledgeDeliveryResponse(boolean acknowledged)
   {
      super(RESP_ACKDELIVERY);

      this.acknowledged = acknowledged;
   }

   // Public --------------------------------------------------------

   public boolean isAcknowledged()
   {
      return acknowledged;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", acknowledged=" + acknowledged + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

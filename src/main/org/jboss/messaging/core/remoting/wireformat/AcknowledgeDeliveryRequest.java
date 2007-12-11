/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_ACKDELIVERY;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 *  
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgeDeliveryRequest extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long deliveryID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcknowledgeDeliveryRequest(long deliveryID)
   {
      super(REQ_ACKDELIVERY);

      this.deliveryID = deliveryID;
   }

   // Public --------------------------------------------------------

   public long getDeliveryID()
   {
      return deliveryID;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", deliveryID=" + deliveryID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

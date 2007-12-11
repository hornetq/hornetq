/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_RECOVERDELIVERIES;

import java.util.List;

import org.jboss.jms.delegate.DeliveryRecovery;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class RecoverDeliveriesMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final List<DeliveryRecovery> deliveries;
   private final String sessionID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RecoverDeliveriesMessage(List<DeliveryRecovery> deliveries,
         String sessionID)
   {
      super(MSG_RECOVERDELIVERIES);

      assert deliveries != null;
      assert deliveries.size() > 0;
      assertValidID(sessionID);

      this.deliveries = deliveries;
      this.sessionID = sessionID;
   }

   // Public --------------------------------------------------------

   public List<DeliveryRecovery> getDeliveries()
   {
      return deliveries;
   }

   public String getSessionID()
   {
      return sessionID;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", deliveries=" + deliveries + ", sessionID="
            + sessionID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

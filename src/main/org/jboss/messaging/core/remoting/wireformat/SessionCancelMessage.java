/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CANCEL;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionCancelMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final long deliveryID;
   
   private final boolean expired;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCancelMessage(final long deliveryID, final boolean expired)
   {
      super(SESS_CANCEL);
      
      this.deliveryID = deliveryID;
      
      this.expired = expired;
   }

   // Public --------------------------------------------------------
   
   public long getDeliveryID()
   {
      return deliveryID;
   }
   
   public boolean isExpired()
   {
      return expired;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", deliveryID=" + deliveryID + ", expired=" + expired + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionAcknowledgeMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final long deliveryID;
   
   private final boolean allUpTo;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAcknowledgeMessage(final long deliveryID, final boolean allUpTo)
   {
      super(PacketType.SESS_ACKNOWLEDGE);
      
      this.deliveryID = deliveryID;
      
      this.allUpTo = allUpTo;
   }

   // Public --------------------------------------------------------
   
   public long getDeliveryID()
   {
      return deliveryID;
   }
   
   public boolean isAllUpTo()
   {
      return allUpTo;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", deliveryID=" + deliveryID + ", allUpTo=" + allUpTo + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


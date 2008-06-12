/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionAcknowledgeMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private long deliveryID;
   
   private boolean allUpTo;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAcknowledgeMessage(final long deliveryID, final boolean allUpTo)
   {
      super(SESS_ACKNOWLEDGE);
      
      this.deliveryID = deliveryID;
      
      this.allUpTo = allUpTo;
   }
   
   public SessionAcknowledgeMessage()
   {
      super(SESS_ACKNOWLEDGE);
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(deliveryID);
      buffer.putBoolean(allUpTo);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      deliveryID = buffer.getLong();
      allUpTo = buffer.getBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", deliveryID=" + deliveryID + ", allUpTo=" + allUpTo + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionAcknowledgeMessage == false)
      {
         return false;
      }
            
      SessionAcknowledgeMessage r = (SessionAcknowledgeMessage)other;
      
      return this.deliveryID == r.deliveryID &&
             this.allUpTo == r.allUpTo;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


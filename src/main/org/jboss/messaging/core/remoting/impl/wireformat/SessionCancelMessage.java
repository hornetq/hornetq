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
public class SessionCancelMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private long deliveryID;
   
   private boolean expired;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCancelMessage(final long deliveryID, final boolean expired)
   {
      super(SESS_CANCEL);
      
      this.deliveryID = deliveryID;
      
      this.expired = expired;
   }
   
   public SessionCancelMessage()
   {
      super(SESS_CANCEL);
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(deliveryID);
      buffer.putBoolean(expired);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      deliveryID = buffer.getLong();
      expired = buffer.getBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", deliveryID=" + deliveryID + ", expired=" + expired + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionCancelMessage == false)
      {
         return false;
      }
            
      SessionCancelMessage r = (SessionCancelMessage)other;
      
      return this.deliveryID == r.deliveryID &&
             this.expired == r.expired;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


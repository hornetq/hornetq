/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;


/**
 * 
 * A SessionRemoveDestinationMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionRemoveDestinationMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private SimpleString address;
   
   private boolean temporary;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public SessionRemoveDestinationMessage(final SimpleString address, final boolean temporary)
   {
      super(SESS_REMOVE_DESTINATION);
      
      this.address = address;
      
      this.temporary = temporary;
   }
   
   public SessionRemoveDestinationMessage()
   {
      super(SESS_REMOVE_DESTINATION);
   }

   // Public --------------------------------------------------------
   
   public SimpleString getAddress()
   {
      return address;
   }
   
   public boolean isTemporary()
   {
   	return temporary;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putSimpleString(address);
      buffer.putBoolean(temporary);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      address = buffer.getSimpleString();
      temporary = buffer.getBoolean();
   }
      
   @Override
   public String toString()
   {
      return getParentString() + ", address=" + address + ", temp=" + temporary + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionRemoveDestinationMessage == false)
      {
         return false;
      }
            
      SessionRemoveDestinationMessage r = (SessionRemoveDestinationMessage)other;
      
      return this.address.equals(r.address) &&
             this.temporary == r.temporary;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}



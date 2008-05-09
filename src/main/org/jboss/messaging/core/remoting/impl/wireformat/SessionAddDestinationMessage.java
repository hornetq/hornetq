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
 * A SessionAddDestinationMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionAddDestinationMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private SimpleString address;
   
   private boolean temporary;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAddDestinationMessage(final SimpleString address, final boolean temp)
   {
      super(SESS_ADD_DESTINATION);
      
      this.address = address;
      
      this.temporary = temp;
   }
   
   public SessionAddDestinationMessage()
   {
      super(SESS_ADD_DESTINATION);
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
      return getParentString() + ", address=" + address + ", temp=" + temporary +"]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


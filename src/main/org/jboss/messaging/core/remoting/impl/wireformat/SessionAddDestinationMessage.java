/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_ADD_DESTINATION;


/**
 * 
 * A SessionAddDestinationMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionAddDestinationMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final String address;
   
   private final boolean temporary;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAddDestinationMessage(final String address, final boolean temp)
   {
      super(SESS_ADD_DESTINATION);
      
      this.address = address;
      
      this.temporary = temp;
   }

   // Public --------------------------------------------------------
   
   public String getAddress()
   {
      return address;
   }
   
   public boolean isTemporary()
   {
   	return temporary;
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


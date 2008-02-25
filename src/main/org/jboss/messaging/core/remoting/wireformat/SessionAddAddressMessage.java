/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ADD_ADDRESS;


/**
 * 
 * A SessionAddAddressMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionAddAddressMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final String address;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAddAddressMessage(final String address)
   {
      super(SESS_ADD_ADDRESS);
      
      this.address = address;
   }

   // Public --------------------------------------------------------
   
   public String getAddress()
   {
      return address;
   }
   
   @Override
   public String toString()
   {
      return getParentString() + ", address=" + address + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


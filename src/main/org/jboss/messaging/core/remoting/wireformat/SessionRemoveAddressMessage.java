/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_REMOVE_ADDRESS;


/**
 * 
 * A SessionRemoveAddressMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionRemoveAddressMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final String address;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public SessionRemoveAddressMessage(final String address)
   {
      super(SESS_REMOVE_ADDRESS);
      
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



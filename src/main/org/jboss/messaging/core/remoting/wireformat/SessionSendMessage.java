/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_SEND;

import org.jboss.messaging.core.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String address;
   
   private final Message message;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendMessage(String address, Message message)
   {
      super(SESS_SEND);

      assert message != null;
            
      this.address = address;
      
      this.message = message;
   }

   // Public --------------------------------------------------------

   public Message getMessage()
   {
      return message;
   }
   
   public String getAddress()
   {
      return address;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", message=" + message + ", address=" + address
            + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

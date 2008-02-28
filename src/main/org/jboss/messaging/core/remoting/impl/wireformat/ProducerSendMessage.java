/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_SEND;

import org.jboss.messaging.core.message.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ProducerSendMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

	private String address;
	
   private final Message message;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerSendMessage(final String address, final Message message)
   {
      super(PROD_SEND);

      this.address = address;
      
      this.message = message;
   }

   // Public --------------------------------------------------------

   public String getAddress()
   {
   	return address;
   }
   
   public Message getMessage()
   {
      return message;
   }
   
   @Override
   public String toString()
   {
      return getParentString() + ", address=" + address + ", message=" + message
            + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

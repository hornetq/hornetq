/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;

import org.jboss.messaging.core.Destination;
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

   private final Message message;
   private final Destination destination;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendMessage(Message message, Destination destination)
   {
      super(MSG_SENDMESSAGE);

      assert message != null;

      this.message = message;
      this.destination = destination;
   }

   // Public --------------------------------------------------------

   public Message getMessage()
   {
      return message;
   }
   
   public Destination getDestination()
   {
      return destination;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", message=" + message
            + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

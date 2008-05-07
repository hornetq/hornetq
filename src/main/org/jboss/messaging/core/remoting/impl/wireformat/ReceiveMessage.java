/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.RECEIVE_MSG;

import org.jboss.messaging.core.message.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ReceiveMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Message message;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReceiveMessage(final Message message)
   {
      super(RECEIVE_MSG);

      assert message != null;

      this.message = message;
   }

   // Public --------------------------------------------------------

   public Message getMessage()
   {
      return message;
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", message=" + message);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

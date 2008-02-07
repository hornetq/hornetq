/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import org.jboss.messaging.core.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionBrowserNextMessageResponseMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Message message;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBrowserNextMessageResponseMessage(Message message)
   {
      super(PacketType.SESS_BROWSER_NEXTMESSAGE_RESP);

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
      return getParentString() + ", message=" + message + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

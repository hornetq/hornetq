/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class SessionBrowserNextMessageBlockMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long maxMessages;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBrowserNextMessageBlockMessage(final long maxMessages)
   {
      super(SESS_BROWSER_NEXTMESSAGEBLOCK);

      this.maxMessages = maxMessages;
   }

   // Public --------------------------------------------------------

   public long getMaxMessages()
   {
      return maxMessages;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", maxMessages=" + maxMessages + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

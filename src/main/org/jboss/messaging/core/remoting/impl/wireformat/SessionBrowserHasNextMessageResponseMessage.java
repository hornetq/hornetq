/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE_RESP;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SessionBrowserHasNextMessageResponseMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final boolean hasNext;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBrowserHasNextMessageResponseMessage(final boolean hasNext)
   {
      super(SESS_BROWSER_HASNEXTMESSAGE_RESP);

      this.hasNext = hasNext;
   }

   // Public --------------------------------------------------------

   public boolean hasNext()
   {
      return hasNext;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", hasNext=" + hasNext + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

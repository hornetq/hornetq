/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_HASNEXTMESSAGE;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class BrowserHasNextMessageResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final boolean hasNext;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BrowserHasNextMessageResponse(boolean hasNext)
   {
      super(RESP_BROWSER_HASNEXTMESSAGE);

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

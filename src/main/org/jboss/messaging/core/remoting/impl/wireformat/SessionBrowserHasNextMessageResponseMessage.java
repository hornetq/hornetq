/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SessionBrowserHasNextMessageResponseMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean hasNext;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBrowserHasNextMessageResponseMessage(final boolean hasNext)
   {
      super(SESS_BROWSER_HASNEXTMESSAGE_RESP);

      this.hasNext = hasNext;
   }
   
   public SessionBrowserHasNextMessageResponseMessage()
   {
      super(SESS_BROWSER_HASNEXTMESSAGE_RESP);
   }

   // Public --------------------------------------------------------

   public boolean hasNext()
   {
      return hasNext;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putBoolean(hasNext);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      hasNext = buffer.getBoolean();       
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

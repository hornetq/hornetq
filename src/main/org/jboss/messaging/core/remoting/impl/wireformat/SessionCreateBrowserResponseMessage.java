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
public class SessionCreateBrowserResponseMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long browserTargetID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateBrowserResponseMessage(final long browserTargetID)
   {
      super(SESS_CREATEBROWSER_RESP);

      this.browserTargetID = browserTargetID;
   }
   
   public SessionCreateBrowserResponseMessage()
   {
      super(SESS_CREATEBROWSER_RESP);
   }

   // Public --------------------------------------------------------

   public long getBrowserTargetID()
   {
      return browserTargetID;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(browserTargetID);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      browserTargetID = buffer.getLong();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", browserTargetID=" + browserTargetID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

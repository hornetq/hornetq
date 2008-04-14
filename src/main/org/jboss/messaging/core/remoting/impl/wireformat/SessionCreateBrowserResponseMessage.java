/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER_RESP;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SessionCreateBrowserResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long browserTargetID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateBrowserResponseMessage(final long browserTargetID)
   {
      super(SESS_CREATEBROWSER_RESP);

      this.browserTargetID = browserTargetID;
   }

   // Public --------------------------------------------------------

   public long getBrowserTargetID()
   {
      return browserTargetID;
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

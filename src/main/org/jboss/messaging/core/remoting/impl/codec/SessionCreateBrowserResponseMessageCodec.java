/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionCreateBrowserResponseMessageCodec extends
      AbstractPacketCodec<SessionCreateBrowserResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateBrowserResponseMessageCodec()
   {
      super(SESS_CREATEBROWSER_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final SessionCreateBrowserResponseMessage response,
         final MessagingBuffer out) throws Exception
   {
      long browserID = response.getBrowserTargetID();

      out.putLong(browserID);
   }

   @Override
   protected SessionCreateBrowserResponseMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      long browserID = in.getLong();

      return new SessionCreateBrowserResponseMessage(browserID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

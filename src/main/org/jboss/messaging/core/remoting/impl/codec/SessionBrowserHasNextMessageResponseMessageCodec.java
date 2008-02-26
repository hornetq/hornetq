/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class SessionBrowserHasNextMessageResponseMessageCodec extends
      AbstractPacketCodec<SessionBrowserHasNextMessageResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBrowserHasNextMessageResponseMessageCodec()
   {
      super(SESS_BROWSER_HASNEXTMESSAGE_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionBrowserHasNextMessageResponseMessage response,
         RemotingBuffer out) throws Exception
   {
      out.putInt(1); //body length
      out.putBoolean(response.hasNext());
   }

   @Override
   protected SessionBrowserHasNextMessageResponseMessage decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      boolean hasNext = in.getBoolean();

      return new SessionBrowserHasNextMessageResponseMessage(hasNext);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

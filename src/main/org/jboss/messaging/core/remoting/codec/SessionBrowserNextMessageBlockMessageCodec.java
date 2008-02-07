/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK;

import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageBlockMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class SessionBrowserNextMessageBlockMessageCodec extends AbstractPacketCodec<SessionBrowserNextMessageBlockMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBrowserNextMessageBlockMessageCodec()
   {
      super(SESS_BROWSER_NEXTMESSAGEBLOCK);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionBrowserNextMessageBlockMessage request, RemotingBuffer out) throws Exception
   {
      out.putInt(LONG_LENGTH);
      out.putLong(request.getMaxMessages());
   }

   @Override
   protected SessionBrowserNextMessageBlockMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      long maxMessages = in.getLong();
      
      return new SessionBrowserNextMessageBlockMessage(maxMessages);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageBlockMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   public int getBodyLength(final SessionBrowserNextMessageBlockMessage packet) throws Exception
   {   	
      return LONG_LENGTH;
   }
   
   @Override
   protected void encodeBody(final SessionBrowserNextMessageBlockMessage request, final RemotingBuffer out) throws Exception
   {
      out.putLong(request.getMaxMessages());
   }

   @Override
   protected SessionBrowserNextMessageBlockMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      long maxMessages = in.getLong();
      
      return new SessionBrowserNextMessageBlockMessage(maxMessages);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_NEXTMESSAGEBLOCK;

import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class BrowserNextMessageBlockRequestCodec extends AbstractPacketCodec<BrowserNextMessageBlockRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BrowserNextMessageBlockRequestCodec()
   {
      super(REQ_BROWSER_NEXTMESSAGEBLOCK);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(BrowserNextMessageBlockRequest request, RemotingBuffer out) throws Exception
   {
      out.putInt(LONG_LENGTH);
      out.putLong(request.getMaxMessages());
   }

   @Override
   protected BrowserNextMessageBlockRequest decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      long maxMessages = in.getLong();
      
      return new BrowserNextMessageBlockRequest(maxMessages);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_HASNEXTMESSAGE;

import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class BrowserHasNextMessageResponseCodec extends
      AbstractPacketCodec<BrowserHasNextMessageResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BrowserHasNextMessageResponseCodec()
   {
      super(RESP_BROWSER_HASNEXTMESSAGE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(BrowserHasNextMessageResponse response,
         RemotingBuffer out) throws Exception
   {
      out.putInt(1); //body length
      out.putBoolean(response.hasNext());
   }

   @Override
   protected BrowserHasNextMessageResponse decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      boolean hasNext = in.getBoolean();

      return new BrowserHasNextMessageResponse(hasNext);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

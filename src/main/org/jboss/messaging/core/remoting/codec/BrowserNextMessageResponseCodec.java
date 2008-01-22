/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_NEXTMESSAGE;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class BrowserNextMessageResponseCodec extends AbstractPacketCodec<BrowserNextMessageResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BrowserNextMessageResponseCodec()
   {
      super(RESP_BROWSER_NEXTMESSAGE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(BrowserNextMessageResponse response, RemotingBuffer out) throws Exception
   {      
      byte[] encodedMsg = encodeMessage(response.getMessage());

      int bodyLength = INT_LENGTH + encodedMsg.length;

      out.putInt(bodyLength);      
      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
   }

   @Override
   protected BrowserNextMessageResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int msgLength = in.getInt();
      byte[] encodedMsg = new byte[msgLength];
      in.get(encodedMsg);
      Message message = decodeMessage(encodedMsg);

      return new BrowserNextMessageResponse(message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

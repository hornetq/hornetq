/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGE_RESP;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageResponseMessage;
import org.jboss.messaging.util.StreamUtils;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionBrowserNextMessageResponseMessageCodec extends AbstractPacketCodec<SessionBrowserNextMessageResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBrowserNextMessageResponseMessageCodec()
   {
      super(SESS_BROWSER_NEXTMESSAGE_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   //TODO remove this in next refactoring
   private byte[] encodedMsg;
   
   protected int getBodyLength(final SessionBrowserNextMessageResponseMessage packet) throws Exception
   {   	
   	byte[] encodedMsg = StreamUtils.toBytes(packet.getMessage());

      int bodyLength = INT_LENGTH + encodedMsg.length;
      
      return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionBrowserNextMessageResponseMessage response, final RemotingBuffer out) throws Exception
   {         
      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
      encodedMsg = null;
   }

   @Override
   protected SessionBrowserNextMessageResponseMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      int msgLength = in.getInt();
      byte[] encodedMsg = new byte[msgLength];
      in.get(encodedMsg);
      Message message = new MessageImpl();
      StreamUtils.fromBytes(message, encodedMsg);

      return new SessionBrowserNextMessageResponseMessage(message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

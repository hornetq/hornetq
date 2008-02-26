/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_SEND;

import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.server.Message;
import org.jboss.messaging.core.server.impl.MessageImpl;
import org.jboss.messaging.util.StreamUtils;

/**
 * 
 * A ProducerSendMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ProducerSendMessageCodec extends AbstractPacketCodec<ProducerSendMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerSendMessageCodec()
   {
      super(PROD_SEND);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(ProducerSendMessage message, RemotingBuffer out) throws Exception
   {
      byte[] encodedMsg = StreamUtils.toBytes(message.getMessage());   

      int bodyLength = INT_LENGTH + sizeof(message.getAddress()) + encodedMsg.length;

      out.putInt(bodyLength);
      out.putNullableString(message.getAddress());
      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
   }

   @Override
   protected ProducerSendMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String address = in.getNullableString();
      int msgLength = in.getInt();
      byte[] encodedMsg = new byte[msgLength];
      in.get(encodedMsg);
      Message message = new MessageImpl();
      StreamUtils.fromBytes(message, encodedMsg);

      return new ProducerSendMessage(address, message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


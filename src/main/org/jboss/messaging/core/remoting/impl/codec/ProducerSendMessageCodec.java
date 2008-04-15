/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_SEND;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
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
   
   //TOD remove this in next stage of refactoring
   private byte[] encodedMsg;
   
   protected int getBodyLength(final ProducerSendMessage packet) throws Exception
   {
   	encodedMsg = StreamUtils.toBytes(packet.getMessage());   

      int bodyLength = sizeof(packet.getAddress()) + INT_LENGTH + encodedMsg.length;
      
      return bodyLength;
   }

   @Override
   protected void encodeBody(final ProducerSendMessage message, final RemotingBuffer out) throws Exception
   {
      out.putNullableString(message.getAddress());
      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
      encodedMsg = null;
   }

   @Override
   protected ProducerSendMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
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


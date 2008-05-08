/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_SEND;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.ServerMessage;
import org.jboss.messaging.core.message.impl.ServerMessageImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.util.MessagingBuffer;

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

   private static final Logger log = Logger.getLogger(ProducerSendMessageCodec.class);
   
   
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
   protected void encodeBody(final ProducerSendMessage message, final MessagingBuffer out) throws Exception
   {
      MessagingBuffer buffer = message.getClientMessage().encode();
      
      buffer.flip();
      
      //TODO - can be optimised
      
      byte[] data = buffer.array();
      
      out.putBytes(data, 0, buffer.limit());
   }

   @Override
   protected ProducerSendMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      //TODO can be optimised
      
      ServerMessage message = new ServerMessageImpl();
      
      message.decode(in);
      
      message.getBody().flip();

      return new ProducerSendMessage(message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_RECEIVETOKENS;

import org.jboss.messaging.core.remoting.impl.wireformat.ProducerReceiveTokensMessage;

/**
 * 
 * A ProducerReceiveTokensMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ProducerReceiveTokensMessageCodec extends AbstractPacketCodec<ProducerReceiveTokensMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerReceiveTokensMessageCodec()
   {
      super(PROD_RECEIVETOKENS);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(ProducerReceiveTokensMessage message, RemotingBuffer out) throws Exception
   {
      out.putInt(INT_LENGTH);
      out.putInt(message.getTokens());
   }

   @Override
   protected ProducerReceiveTokensMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      return new ProducerReceiveTokensMessage(in.getInt());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_RECEIVETOKENS;

import org.jboss.messaging.core.remoting.impl.wireformat.ProducerReceiveTokensMessage;
import org.jboss.messaging.util.MessagingBuffer;

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
   protected void encodeBody(final ProducerReceiveTokensMessage message, final MessagingBuffer out) throws Exception
   {
      out.putInt(message.getTokens());
   }

   @Override
   protected ProducerReceiveTokensMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      return new ProducerReceiveTokensMessage(in.getInt());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONS_FLOWTOKEN;

import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConsumerFlowTokenMessageCodec extends AbstractPacketCodec<ConsumerFlowTokenMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerFlowTokenMessageCodec()
   {
      super(CONS_FLOWTOKEN);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------
   
   @Override
   protected void encodeBody(final ConsumerFlowTokenMessage message, final MessagingBuffer out) throws Exception
   {
      out.putInt(message.getTokens());
   }

   @Override
   protected ConsumerFlowTokenMessage decodeBody(final MessagingBuffer in) throws Exception
   {
      return new ConsumerFlowTokenMessage(in.getInt());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

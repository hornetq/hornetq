/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONS_FLOWTOKEN;

import org.jboss.messaging.core.remoting.wireformat.ConsumerFlowTokenMessage;

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
   protected void encodeBody(ConsumerFlowTokenMessage message, RemotingBuffer out) throws Exception
   {
      out.putInt(INT_LENGTH);
      out.putInt(message.getTokens());
   }

   @Override
   protected ConsumerFlowTokenMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      return new ConsumerFlowTokenMessage(in.getInt());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

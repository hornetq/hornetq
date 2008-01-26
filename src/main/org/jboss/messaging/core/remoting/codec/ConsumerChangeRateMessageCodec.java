/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CHANGERATE;

import org.jboss.messaging.core.remoting.wireformat.ConsumerChangeRateMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class ConsumerChangeRateMessageCodec extends
      AbstractPacketCodec<ConsumerChangeRateMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerChangeRateMessageCodec()
   {
      super(MSG_CHANGERATE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(ConsumerChangeRateMessage message, RemotingBuffer out) throws Exception
   {
      out.putInt(FLOAT_LENGTH);
      out.putFloat(message.getRate());
   }

   @Override
   protected ConsumerChangeRateMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      float rate = in.getFloat();

      return new ConsumerChangeRateMessage(rate);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

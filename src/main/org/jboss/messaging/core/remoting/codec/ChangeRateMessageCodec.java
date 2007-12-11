/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CHANGERATE;

import org.jboss.messaging.core.remoting.wireformat.ChangeRateMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class ChangeRateMessageCodec extends
      AbstractPacketCodec<ChangeRateMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ChangeRateMessageCodec()
   {
      super(MSG_CHANGERATE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(ChangeRateMessage message, RemotingBuffer out) throws Exception
   {
      out.putInt(FLOAT_LENGTH);
      out.putFloat(message.getRate());
   }

   @Override
   protected ChangeRateMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      float rate = in.getFloat();

      return new ChangeRateMessage(rate);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

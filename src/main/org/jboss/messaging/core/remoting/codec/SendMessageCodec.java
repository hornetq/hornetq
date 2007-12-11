/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;

import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.remoting.wireformat.SendMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class SendMessageCodec extends AbstractPacketCodec<SendMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SendMessageCodec()
   {
      super(MSG_SENDMESSAGE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SendMessage message, RemotingBuffer out) throws Exception
   {
      byte[] encodedMsg = encode(message.getMessage());
      boolean checkForDuplicates = message.checkForDuplicates();
      long sequence = message.getSequence();

      int bodyLength = INT_LENGTH + 1 + encodedMsg.length + 1 + LONG_LENGTH;

      out.putInt(bodyLength);
      out.put(message.getMessage().getType());
      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
      out.putBoolean(checkForDuplicates);
      out.putLong(sequence);
   }

   @Override
   protected SendMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      byte msgType = in.get();
      int msgLength = in.getInt();
      byte[] encodedMsg = new byte[msgLength];
      in.get(encodedMsg);
      JBossMessage msg = (JBossMessage) decode(msgType, encodedMsg);
      boolean checkForDuplicates = in.getBoolean();
      long sequence = in.getLong();

      return new SendMessage(msg, checkForDuplicates, sequence);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;

import org.jboss.messaging.core.remoting.wireformat.SendMessage;
import org.jboss.messaging.newcore.Message;

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
      byte[] encodedMsg = encodeMessage(message.getMessage());
      boolean checkForDuplicates = message.checkForDuplicates();
      long sequence = message.getSequence();

      int bodyLength = INT_LENGTH + 1 + encodedMsg.length + LONG_LENGTH;

      out.putInt(bodyLength);
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

      int msgLength = in.getInt();
      byte[] encodedMsg = new byte[msgLength];
      in.get(encodedMsg);
      Message msg = decodeMessage(encodedMsg);
      boolean checkForDuplicates = in.getBoolean();
      long sequence = in.getLong();

      return new SendMessage(msg, checkForDuplicates, sequence);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

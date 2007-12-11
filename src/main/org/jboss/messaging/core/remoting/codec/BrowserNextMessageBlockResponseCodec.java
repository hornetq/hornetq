/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_NEXTMESSAGEBLOCK;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.impl.message.MessageFactory;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class BrowserNextMessageBlockResponseCodec extends AbstractPacketCodec<BrowserNextMessageBlockResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BrowserNextMessageBlockResponseCodec()
   {
      super(RESP_BROWSER_NEXTMESSAGEBLOCK);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(BrowserNextMessageBlockResponse response, RemotingBuffer out) throws Exception
   {
      JBossMessage[] messages = response.getMessages();
      
      byte[] encodedMessages = encode(messages);

      int bodyLength = INT_LENGTH + INT_LENGTH + encodedMessages.length;

      out.putInt(bodyLength);
      out.putInt(messages.length);
      out.putInt(encodedMessages.length);
      out.put(encodedMessages);
   }

   @Override
   protected BrowserNextMessageBlockResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int numOfMessages = in.getInt();
      int encodedMessagesLength = in.getInt();
      byte[] encodedMessages = new byte[encodedMessagesLength];
      in.get(encodedMessages);
      JBossMessage[] messages = decode(numOfMessages, encodedMessages);

      return new BrowserNextMessageBlockResponse(messages);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   private byte[] encode(JBossMessage[] messages) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream daos = new DataOutputStream(baos);
      
      for (int i = 0; i < messages.length; i++)
      {
         JBossMessage message = messages[i];
         daos.writeByte(message.getType());
         message.write(daos);
      }
      return baos.toByteArray();
   }

   private JBossMessage[] decode(int numOfMessages, byte[] encodedMessages) throws Exception
   {
      JBossMessage[] messages = new JBossMessage[numOfMessages];
      ByteArrayInputStream bais = new ByteArrayInputStream(encodedMessages);
      DataInputStream dais = new DataInputStream(bais);
      
      for (int i = 0; i < messages.length; i++)
      {
         byte type = (byte) dais.readByte();
         JBossMessage message = (JBossMessage)MessageFactory.createMessage(type);         
         message.read(dais);
         messages[i] = message;
      }
      
      return messages;
   }

   // Inner classes -------------------------------------------------
}

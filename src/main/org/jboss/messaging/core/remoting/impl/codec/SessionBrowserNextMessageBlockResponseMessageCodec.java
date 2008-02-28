/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK_RESP;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageBlockResponseMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class SessionBrowserNextMessageBlockResponseMessageCodec extends AbstractPacketCodec<SessionBrowserNextMessageBlockResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static byte[] encode(Message[] messages) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream daos = new DataOutputStream(baos);
      
      for (int i = 0; i < messages.length; i++)
      {
         Message message = messages[i];
         message.write(daos);
      }
      return baos.toByteArray();
   }

   // Constructors --------------------------------------------------

   public SessionBrowserNextMessageBlockResponseMessageCodec()
   {
      super(SESS_BROWSER_NEXTMESSAGEBLOCK_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionBrowserNextMessageBlockResponseMessage response, RemotingBuffer out) throws Exception
   {
      Message[] messages = response.getMessages();
      
      byte[] encodedMessages = encode(messages);

      int bodyLength = INT_LENGTH + INT_LENGTH + encodedMessages.length;

      out.putInt(bodyLength);
      out.putInt(messages.length);
      out.putInt(encodedMessages.length);
      out.put(encodedMessages);
   }

   @Override
   protected SessionBrowserNextMessageBlockResponseMessage decodeBody(RemotingBuffer in)
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
      Message[] messages = decode(numOfMessages, encodedMessages);

      return new SessionBrowserNextMessageBlockResponseMessage(messages);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   private Message[] decode(int numOfMessages, byte[] encodedMessages) throws Exception
   {
      Message[] messages = new Message[numOfMessages];
      ByteArrayInputStream bais = new ByteArrayInputStream(encodedMessages);
      DataInputStream dais = new DataInputStream(bais);
      
      for (int i = 0; i < messages.length; i++)
      {
         Message message = new MessageImpl();
         message.read(dais);
         messages[i] = message;
      }
      
      return messages;
   }

   // Inner classes -------------------------------------------------
}

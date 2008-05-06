/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK_RESP;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageBlockResponseMessage;
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionBrowserNextMessageBlockResponseMessageCodec extends AbstractPacketCodec<SessionBrowserNextMessageBlockResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static byte[] encode(final Message[] messages) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream daos = new DataOutputStream(baos);
      
      daos.writeInt(messages.length);
      
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

   //TODO remove this in next refactoring
   private byte[] encodedMsgs;
   
   public int getBodyLength(final SessionBrowserNextMessageBlockResponseMessage packet) throws Exception
   {   	
   	Message[] messages = packet.getMessages();
      
      encodedMsgs = encode(messages);

      int bodyLength = SIZE_INT + encodedMsgs.length;
      
      return bodyLength;
   }
   
   @Override
   protected void encodeBody(SessionBrowserNextMessageBlockResponseMessage response, RemotingBuffer out) throws Exception
   {
      out.putInt(encodedMsgs.length);
      out.put(encodedMsgs);
      encodedMsgs = null;
   }

   @Override
   protected SessionBrowserNextMessageBlockResponseMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      int encodedMessagesLength = in.getInt();
      byte[] encodedMessages = new byte[encodedMessagesLength];
      in.get(encodedMessages);
      Message[] messages = decode(encodedMessages);

      return new SessionBrowserNextMessageBlockResponseMessage(messages);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   private Message[] decode(final byte[] encodedMessages) throws Exception
   {
      
      ByteArrayInputStream bais = new ByteArrayInputStream(encodedMessages);
      DataInputStream dais = new DataInputStream(bais);
      
      int numOfMessages = dais.readInt();
      Message[] messages = new Message[numOfMessages];
      
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

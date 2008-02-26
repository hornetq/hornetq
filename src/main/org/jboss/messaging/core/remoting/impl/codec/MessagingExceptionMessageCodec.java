/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.EXCEPTION;

import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.server.MessagingException;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class MessagingExceptionMessageCodec extends AbstractPacketCodec<MessagingExceptionMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MessagingExceptionMessageCodec()
   {
      super(EXCEPTION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(MessagingExceptionMessage message, RemotingBuffer out) throws Exception
   {
      int code = message.getException().getCode();
      String msg = message.getException().getMessage();

      int bodyLength = INT_LENGTH + sizeof(msg);
      
      out.putInt(bodyLength);
      out.putInt(code);
      out.putNullableString(msg);
   }

   @Override
   protected MessagingExceptionMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int code = in.getInt();
      String msg = in.getNullableString();

      MessagingException me = new MessagingException(code, msg);
      return new MessagingExceptionMessage(me);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

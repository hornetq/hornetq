/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.EXCEPTION;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   public int getBodyLength(final MessagingExceptionMessage packet) throws Exception
   {
   	return SIZE_INT + sizeof(packet.getException().getMessage());
   }
   
   @Override
   protected void encodeBody(final MessagingExceptionMessage message, final RemotingBuffer out) throws Exception
   {
      out.putInt(message.getException().getCode());
      out.putNullableString(message.getException().getMessage());
   }

   @Override
   protected MessagingExceptionMessage decodeBody(final RemotingBuffer in) throws Exception
   {
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

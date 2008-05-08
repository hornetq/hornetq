/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.RECEIVE_MSG;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ReceiveMessageCodec extends AbstractPacketCodec<ReceiveMessage>
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReceiveMessageCodec.class);
      
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReceiveMessageCodec()
   {
      super(RECEIVE_MSG);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final ReceiveMessage message, final MessagingBuffer out) throws Exception
   {
 
      MessagingBuffer buffer = message.getMessage().encode();
      
      buffer.flip();
      
      //TODO - can be optimised
      
      byte[] data = buffer.array();
      
      out.putBytes(data, 0, buffer.limit());
   }

   @Override
   protected ReceiveMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      //TODO can be optimised
      
      Message message = new MessageImpl();
      
      message.decode(in);
      
      message.getBody().flip();

      return new ReceiveMessage(message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

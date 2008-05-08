/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_QUEUEQUERY_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SessionQueueQueryResponseMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryResponseMessageCodec extends AbstractPacketCodec<SessionQueueQueryResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionQueueQueryResponseMessageCodec()
   {
      super(SESS_QUEUEQUERY_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final SessionQueueQueryResponseMessage message, final MessagingBuffer out) throws Exception
   {
      boolean exists = message.isExists();
      boolean durable = message.isDurable();
      boolean temporary = message.isTemporary();
      int maxSize = message.getMaxSize();
      int consumerCount = message.getConsumerCount();
      int messageCount = message.getMessageCount();
      SimpleString filterString  = message.getFilterString();
      SimpleString address = message.getAddress();
     
      out.putBoolean(exists);
      out.putBoolean(durable);
      out.putBoolean(temporary);
      out.putInt(maxSize);
      out.putInt(consumerCount);
      out.putInt(messageCount);
      out.putNullableSimpleString(filterString);
      out.putNullableSimpleString(address);
   }

   @Override
   protected SessionQueueQueryResponseMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      boolean exists = in.getBoolean();
      boolean durable = in.getBoolean();
      boolean temporary = in.getBoolean();
      int maxSize = in.getInt();
      int consumerCount = in.getInt();
      int messageCount = in.getInt();
      SimpleString filterString  = in.getNullableSimpleString();
      SimpleString address = in.getNullableSimpleString();
    
      if (exists)
      {
         return new SessionQueueQueryResponseMessage(durable, temporary, maxSize, consumerCount,
                                       messageCount, filterString, address);
      }
      else
      {
         return new SessionQueueQueryResponseMessage();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}



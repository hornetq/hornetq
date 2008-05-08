/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEQUEUE;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SessionCreateQueueMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionCreateQueueMessageCodec extends AbstractPacketCodec<SessionCreateQueueMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateQueueMessageCodec()
   {
      super(SESS_CREATEQUEUE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------
 
   @Override
   protected void encodeBody(final SessionCreateQueueMessage message, final MessagingBuffer out) throws Exception
   {
      SimpleString address = message.getAddress();
      SimpleString queueName = message.getQueueName();
      SimpleString filterString = message.getFilterString();
      boolean durable = message.isDurable();
      boolean temporary = message.isTemporary();
     
      out.putSimpleString(address);
      out.putSimpleString(queueName);
      out.putNullableSimpleString(filterString);
      out.putBoolean(durable);
      out.putBoolean(temporary);
   }

   @Override
   protected SessionCreateQueueMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      SimpleString address = in.getSimpleString();
      SimpleString queueName = in.getSimpleString();
      SimpleString filterString = in.getNullableSimpleString();
      boolean durable = in.getBoolean();
      boolean temporary = in.getBoolean();
    
      return new SessionCreateQueueMessage(address, queueName, filterString, durable, temporary);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


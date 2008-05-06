/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEQUEUE;
import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
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

   public int getBodyLength(final SessionCreateQueueMessage packet) throws Exception
   {   	
   	SimpleString address = packet.getAddress();
      SimpleString queueName = packet.getQueueName();
      SimpleString filterString = packet.getFilterString();
   	int bodyLength = SimpleString.sizeofString(address) + SimpleString.sizeofString(queueName) +
   	SimpleString.sizeofNullableString(filterString) + 2 * SIZE_BOOLEAN;
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionCreateQueueMessage message, final RemotingBuffer out) throws Exception
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
   protected SessionCreateQueueMessage decodeBody(final RemotingBuffer in)
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


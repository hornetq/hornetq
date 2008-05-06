/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_DELETE_QUEUE;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SessionDeleteQueueMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionDeleteQueueMessageCodec extends AbstractPacketCodec<SessionDeleteQueueMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionDeleteQueueMessageCodec()
   {
      super(SESS_DELETE_QUEUE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionDeleteQueueMessage packet) throws Exception
   {   	
   	SimpleString queueName = packet.getQueueName();      
      int bodyLength = SimpleString.sizeofString(queueName);
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionDeleteQueueMessage message, final RemotingBuffer out) throws Exception
   {
      SimpleString queueName = message.getQueueName();   
      out.putSimpleString(queueName);
   }

   @Override
   protected SessionDeleteQueueMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      SimpleString queueName = in.getSimpleString();
    
      return new SessionDeleteQueueMessage(queueName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BINDINGQUERY_RESP;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;

/**
 * 
 * A SessionBindingQueryResponseMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryResponseMessageCodec extends AbstractPacketCodec<SessionBindingQueryResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBindingQueryResponseMessageCodec()
   {
      super(SESS_BINDINGQUERY_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionBindingQueryResponseMessage packet) throws Exception
   {   	
      List<String> queueNames = packet.getQueueNames();

      int bodyLength = BOOLEAN_LENGTH + INT_LENGTH;
      
      for (String queueName: queueNames)
      {
         bodyLength += sizeof(queueName);
      }
      
      return bodyLength;
   }

   @Override
   protected void encodeBody(final SessionBindingQueryResponseMessage message, final RemotingBuffer out) throws Exception
   {
      boolean exists = message.isExists();
      List<String> queueNames = message.getQueueNames();

      out.putBoolean(exists);
      out.putInt(queueNames.size());
      
      for (String queueName: queueNames)
      {
         out.putNullableString(queueName);
      }      
   }

   @Override
   protected SessionBindingQueryResponseMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      boolean exists = in.getBoolean();
      
      int numQueues = in.getInt();
      
      List<String> queueNames = new ArrayList<String>(numQueues);
      
      for (int i = 0; i < numQueues; i++)
      {
         queueNames.add(in.getNullableString());
      }
          
      return new SessionBindingQueryResponseMessage(exists, queueNames);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}



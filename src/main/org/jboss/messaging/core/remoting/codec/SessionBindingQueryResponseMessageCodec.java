/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BINDINGQUERY_RESP;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryResponseMessage;

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


   @Override
   protected void encodeBody(SessionBindingQueryResponseMessage message, RemotingBuffer out) throws Exception
   {
      boolean exists = message.isExists();
      List<String> queueNames = message.getQueueNames();

      int bodyLength = 1 + INT_LENGTH;
      
      for (String queueName: queueNames)
      {
         bodyLength += sizeof(queueName);
      }
      
      out.putInt(bodyLength);
      out.putBoolean(exists);
      out.putInt(queueNames.size());
      
      for (String queueName: queueNames)
      {
         out.putNullableString(queueName);
      }      
   }

   @Override
   protected SessionBindingQueryResponseMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

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



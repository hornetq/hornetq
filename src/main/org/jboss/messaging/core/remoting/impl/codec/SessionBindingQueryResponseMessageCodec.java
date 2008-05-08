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
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

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
   protected void encodeBody(final SessionBindingQueryResponseMessage message, final MessagingBuffer out) throws Exception
   {
      boolean exists = message.isExists();
      List<SimpleString> queueNames = message.getQueueNames();

      out.putBoolean(exists);
      out.putInt(queueNames.size());
      
      for (SimpleString queueName: queueNames)
      {
         out.putSimpleString(queueName);
      }      
   }

   @Override
   protected SessionBindingQueryResponseMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      boolean exists = in.getBoolean();
      
      int numQueues = in.getInt();
      
      List<SimpleString> queueNames = new ArrayList<SimpleString>(numQueues);
      
      for (int i = 0; i < numQueues; i++)
      {
         queueNames.add(in.getSimpleString());
      }
          
      return new SessionBindingQueryResponseMessage(exists, queueNames);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}



package org.jboss.messaging.core.remoting.impl.wireformat;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SessionBindingQueryResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryResponseMessage extends PacketImpl
{
   private boolean exists;
   
   private List<SimpleString> queueNames;
   
   public SessionBindingQueryResponseMessage(final boolean exists, final List<SimpleString> queueNames)
   {
      super(SESS_BINDINGQUERY_RESP);

      this.exists = exists;

      this.queueNames = queueNames;
   }
   
   public SessionBindingQueryResponseMessage()
   {
      super(SESS_BINDINGQUERY_RESP);
   }

   public boolean isExists()
   {
      return exists;
   }

   public List<SimpleString> getQueueNames()
   {
      return this.queueNames;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putBoolean(exists);
      buffer.putInt(queueNames.size());      
      for (SimpleString queueName: queueNames)
      {
         buffer.putSimpleString(queueName);
      }      
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      exists = buffer.getBoolean();      
      int numQueues = buffer.getInt();      
      queueNames = new ArrayList<SimpleString>(numQueues);      
      for (int i = 0; i < numQueues; i++)
      {
         queueNames.add(buffer.getSimpleString());
      }          
   }

}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;


/**
 * 
 * A SessionQueueQueryMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryMessage extends EmptyPacket
{
   private SimpleString queueName;

   public SessionQueueQueryMessage(final SimpleString queueName)
   {
      super(SESS_QUEUEQUERY);

      this.queueName = queueName;            
   }
   
   public SessionQueueQueryMessage()
   {
      super(SESS_QUEUEQUERY);        
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putSimpleString(queueName);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      queueName = buffer.getSimpleString();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionQueueQueryMessage == false)
      {
         return false;
      }
            
      SessionQueueQueryMessage r = (SessionQueueQueryMessage)other;
      
      return r.queueName.equals(this.queueName);
   }
   
}

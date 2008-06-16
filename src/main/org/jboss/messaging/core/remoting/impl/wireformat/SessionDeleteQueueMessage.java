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
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class SessionDeleteQueueMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionDeleteQueueMessage(final SimpleString queueName)
   {
      super(SESS_DELETE_QUEUE);

      this.queueName = queueName;
   }
   
   public SessionDeleteQueueMessage()
   {
      super(SESS_DELETE_QUEUE);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append("]");
      return buff.toString();
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
      if (other instanceof SessionDeleteQueueMessage == false)
      {
         return false;
      }
            
      SessionDeleteQueueMessage r = (SessionDeleteQueueMessage)other;
      
      return r.queueName.equals(this.queueName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.SimpleString;


/**
 * 
 * A SessionQueueQueryMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryMessage extends PacketImpl
{
   private final SimpleString queueName;

   public SessionQueueQueryMessage(final SimpleString queueName)
   {
      super(PacketType.SESS_QUEUEQUERY);

      this.queueName = queueName;            
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }
   
}

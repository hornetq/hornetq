/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;


/**
 * 
 * A SessionQueueQueryMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryMessage extends AbstractPacket
{
   private final String queueName;

   public SessionQueueQueryMessage(final String queueName)
   {
      super(PacketType.SESS_QUEUEQUERY);

      this.queueName = queueName;            
   }

   public String getQueueName()
   {
      return queueName;
   }
   
}

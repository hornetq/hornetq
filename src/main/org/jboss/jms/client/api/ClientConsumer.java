/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.util.MessagingException;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientConsumer
{      
   Message receive(long timeout) throws MessagingException;
   
   Message receiveImmediate() throws MessagingException;
   
   MessageHandler getMessageHandler() throws MessagingException;

   void setMessageHandler(MessageHandler handler) throws MessagingException;
   
   String getQueueName();
        
   void close() throws MessagingException;
   
   boolean isClosed();      
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.client;

import org.jboss.messaging.core.exception.MessagingException;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientConsumer
{      
	ClientMessage receive() throws MessagingException;
	
   ClientMessage receive(long timeout) throws MessagingException;
   
   ClientMessage receiveImmediate() throws MessagingException;
   
   MessageHandler getMessageHandler() throws MessagingException;

   void setMessageHandler(MessageHandler handler) throws MessagingException;
   
   void close() throws MessagingException;
   
   boolean isClosed();      
}

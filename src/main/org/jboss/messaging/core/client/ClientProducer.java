/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.client;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public interface ClientProducer
{        
	SimpleString getAddress();
	
	void send(ClientMessage message) throws MessagingException;
	
   void send(SimpleString address, ClientMessage message) throws MessagingException;
   
   void registerAcknowledgementHandler(AcknowledgementHandler handler);
   
   void unregisterAcknowledgementHandler(AcknowledgementHandler handler);
      
   void close() throws MessagingException;
   
   boolean isClosed();   
   
   boolean isBlockOnPersistentSend();
   
   boolean isBlockOnNonPersistentSend();
   
   int getMaxRate();
   
   int getInitialWindowSize();

   void cleanUp();
}

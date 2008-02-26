/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessagingException;
import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;

/**
 * 
 * A ClientSessionInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientSessionInternal extends ClientSession
{
   String getID();
   
   ClientConnectionInternal getConnection();
      
   void delivered(long deliveryID, boolean expired);
   
   void flushAcks() throws MessagingException;
   
   void removeConsumer(ClientConsumerInternal consumer) throws MessagingException;
   
   void removeProducer(ClientProducer producer);
   
   void removeBrowser(ClientBrowser browser);  
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.impl;

import org.jboss.jms.client.api.ClientBrowser;
import org.jboss.jms.client.api.ClientProducer;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.util.MessagingException;

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
   
   void send(String address, Message message) throws MessagingException;   
}

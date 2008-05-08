/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.exception.MessagingException;

/**
 * 
 * A ClientConsumerInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientConsumerInternal extends ClientConsumer
{   
   long getClientTargetID();
   
   void handleMessage(ClientMessage message) throws Exception;
   
   void recover(long lastDeliveryID) throws MessagingException;
}

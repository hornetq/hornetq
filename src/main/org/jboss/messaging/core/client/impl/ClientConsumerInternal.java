/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.util.MessagingException;

/**
 * 
 * A ClientConsumerInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientConsumerInternal extends ClientConsumer
{   
   String getID();
   
   void handleMessage(DeliverMessage message) throws Exception;
   
   void recover(long lastDeliveryID) throws MessagingException;
}

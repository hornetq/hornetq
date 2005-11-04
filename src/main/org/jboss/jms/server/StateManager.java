/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.util.Set;

import javax.jms.JMSException;

import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.messaging.core.local.Topic;

/**
 * A StateManager manages user specific state. E.g. durable subscriptions
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface StateManager
{

   DurableSubscription getDurableSubscription(String clientID, String subscriptionName)
      throws JMSException;
   
   DurableSubscription createDurableSubscription(String topicName, String clientID, String subscriptionName,
         String selector)
      throws JMSException;
   
   boolean removeDurableSubscription(String clientID, String subscriptionName)
      throws JMSException;
   
   String getPreConfiguredClientID(String username) throws JMSException;
   
   Set loadDurableSubscriptionsForTopic(String topicName) throws JMSException;
   
   
}

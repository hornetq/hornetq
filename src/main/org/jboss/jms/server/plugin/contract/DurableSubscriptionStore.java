/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin.contract;

import org.jboss.messaging.core.local.CoreDurableSubscription;
import org.jboss.messaging.core.plugin.contract.ServerPlugin;
import org.jboss.messaging.core.plugin.contract.TransactionLog;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.jms.server.DestinationManager;

import javax.jms.JMSException;
import java.util.Set;

/**
 * An interface to a reliable durable subscription store.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface DurableSubscriptionStore extends ServerPlugin
{
   CoreDurableSubscription createDurableSubscription(String topicName,
                                                 String clientID,
                                                 String subscriptionName,
                                                 String selector,
                                                 boolean noLocal,
                                                 DestinationManager dm,
                                                 MessageStore ms,
                                                 TransactionLog tl) throws JMSException;

   CoreDurableSubscription getDurableSubscription(String clientID,
                                              String subscriptionName,
                                              DestinationManager dm,
                                              MessageStore ms,
                                              TransactionLog tl) throws JMSException;

   boolean removeDurableSubscription(String clientID, String subscriptionName) throws JMSException;

//   /**
//    * Clears the state maintained on behalf of a topic by removing all associated durable
//    * subscriptions. Only to be used when dropping a topic.
//    */
//   void clearSubscriptionsForTopic(String topicName) throws JMSException;

   String getPreConfiguredClientID(String username) throws JMSException;

   Set loadDurableSubscriptionsForTopic(String topicName, 
                                        DestinationManager dm,
                                        MessageStore ms,
                                        TransactionLog tl) throws JMSException;
}

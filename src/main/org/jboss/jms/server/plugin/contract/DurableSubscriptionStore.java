/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin.contract;

import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.messaging.core.plugin.contract.ServerPlugin;
import org.jboss.messaging.core.plugin.contract.TransactionLog;
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
   DurableSubscription createDurableSubscription(String topicName,
                                                 String clientID,
                                                 String subscriptionName,
                                                 String selector,
                                                 boolean noLocal,
                                                 DestinationManager dm,
                                                 MessageStore ms,
                                                 TransactionLog tl) throws JMSException;

   DurableSubscription getDurableSubscription(String clientID,
                                              String subscriptionName,
                                              DestinationManager dm,
                                              MessageStore ms,
                                              TransactionLog tl) throws JMSException;

   boolean removeDurableSubscription(String clientID, String subscriptionName) throws JMSException;

   String getPreConfiguredClientID(String username) throws JMSException;

   Set loadDurableSubscriptionsForTopic(String topicName, 
                                        DestinationManager dm,
                                        MessageStore ms,
                                        TransactionLog tl) throws JMSException;

}

/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin.contract;

import java.util.Set;

import javax.jms.JMSException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.local.CoreDurableSubscription;
import org.jboss.messaging.core.local.CoreSubscription;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.ServerPlugin;

/**
 * Handles mappings between Queues/Topics/Subscriptions and core channels
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>1.1</tt>
 *
 * ChannelMapper.java,v 1.1 2006/02/28 16:48:13 timfox Exp
 */
public interface ChannelMapper extends ServerPlugin
{
   /**
    *  FIXME This doesn't belong here and should be moved out to a different service
    * 
    * @param username
    * @return
    * @throws JMSException
    */
   String getPreConfiguredClientID(String username) throws JMSException;
         
   CoreDestination getCoreDestination(JBossDestination jbDest) throws JMSException;
   
   JBossDestination getJBossDestination(long coreDestinationID);
      
   void deployCoreDestination(boolean isQueue, 
                              String destName, 
                              MessageStore ms, 
                              PersistenceManager pm,
                              int fullSize, 
                              int pageSize, 
                              int downCacheSize) throws JMSException;

   CoreDestination undeployCoreDestination(boolean isQueue, String destName);
   
   CoreDurableSubscription createDurableSubscription(String topicName,
                                                     String clientID,
                                                     String subscriptionName,
                                                     String selector,
                                                     boolean noLocal,
                                                     MessageStore ms,
                                                     PersistenceManager pm) throws JMSException;
   
   CoreSubscription createSubscription(String topicName,
                                       String selector,
                                       boolean noLocal,
                                       MessageStore ms,
                                       PersistenceManager pm) throws JMSException;

   CoreDurableSubscription getDurableSubscription(String clientID,
                                                  String subscriptionName,
                                                  MessageStore ms,
                                                  PersistenceManager pm) throws JMSException;

   boolean removeDurableSubscription(String clientID, String subscriptionName) throws JMSException;

   Set getSubscriptions(String clientID);
   
   //TODO Handle this dependency properly
   void setPersistenceManager(PersistenceManager pm) throws Exception;

}

package org.jboss.jms.server;

import java.util.List;

/**
 * This interface describes what methods are exposed from the ServerPeer to a client via JMX.
 * For statistical info see @see org.jboss.jms.server.JmsServerStatistics
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JmsServer
{
   /**
    * returns the JMS Version this server implements
    * @return  the version
    */
   String getJMSVersion();

   /**
    * returns the JMS major Version this server implements
    * @return  the version
    */
   int getJMSMajorVersion();

   /**
    * returns the JMS Minor Version this server implements
    * @return  the version
    */
   int getJMSMinorVersion();

   /**
    * returns the JMS Provider Name
    * @return  the version
    */
   String getJMSProviderName();

   /**
    * returns the JMS Provider version
    * @return  the version
    */
   String getProviderVersion();

   /**
    * returns the JMS Provider Major version
    * @return the version
    */
   int getProviderMajorVersion();

   /**
    * returns the JMS Provider Minor version
    * @return the version
    */
   int getProviderMinorVersion();

   public Configuration getConfiguration();
   
   /**
    * deploys a new queue
    * @param name the name of the queue
    * @param jndiName the jndi name to bind it against, if null uses the queue name
    * @return the jndi name the queue is bound to
    * @throws Exception if the queue already exists
    */
   String deployQueue(String name, String jndiName) throws Exception;

   /**
    * deploys a new queue
    * @param name the name of the queue
    * @param jndiName the jndi name to bind it against, if null uses the queue name
    * @param fullSize the full size to use for paging
    * @param pageSize the page size
    * @param downCacheSize the down cache size
    * @return the jndi name the queue is bound to
    * @throws Exception if the queue already exists
    */
   String deployQueue(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception;

   /**
    * this undeploys the queue and then completely destroys it.
    * @param name the name of the queue to destroy
    * @return true if the queue was destroyed
    * @throws Exception
    */
   boolean destroyQueue(String name) throws Exception;

   /**
    * undeploys a queue, i.e. unloads and deactivates it without destroying it
    * @param name the name of the queue to undeploy
    * @return true if the queue was undeployed
    * @throws Exception
    */
   boolean undeployQueue(String name) throws Exception;

   /**
    * deploys a new topic
    * @param name the name of the topic
    * @param jndiName the jndi name to bind it against, if null uses the topic name
    * @return the jndi name the topic is bound to
    * @throws Exception if the topic already exists
    */
   String deployTopic(String name, String jndiName) throws Exception;

   /**
    * deploys a new topic
    * @param name the name of the topic
    * @param jndiName the jndi name to bind it against, if null uses the topic name
    * @param fullSize the full size to use for paging
    * @param pageSize the page size
    * @param downCacheSize the down cache size
    * @return the jndi name the topic is bound to
    * @throws Exception if the topic already exists
    */
   String deployTopic(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception;

   /**
    * this undeploys the topic and then completely destroys it.
    * @param name the name of the topic to destroy
    * @return true if the topic was destroyed
    * @throws Exception
    */
   boolean destroyTopic(String name) throws Exception;

   /**
    * undeploys a topic, i.e. unloads and deactivates it without destroying it
    * @param name the name of the topic to undeploy
    * @return true if the topic was undeployed
    * @throws Exception
    */
   boolean undeployTopic(String name) throws Exception;

   /**
    * enables the message counters for all destinations.
    * These statistics can be accessed via @see org.jboss.jms.server.JmsServerStatistics
    */
   void enableMessageCounters();

   /**
    * disables all message counters for all destinations
    */
   void disableMessageCounters();
   /**
    * resets all the message counters
    */
   void resetAllMessageCounters();

   /**
    * resets all the mesage counter histories
    */
   void resetAllMessageCounterHistories();

   /**
    * lists all the prepared transactions
    * @return  the prepared transactions
    */
   List retrievePreparedTransactions();

   public void removeAllMessagesForQueue(String queueName) throws Exception;

   public void removeAllMessagesForTopic(String queueName) throws Exception;

}

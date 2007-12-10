package org.jboss.jms.server;

import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageStatistics;

import java.util.List;

/**
 * This interface describes what statistical methods are exposed to a client via JMX.
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JmsServerStatistics
{
   /**
    * returns how many messages have been recieved for a given queue. This is dependant on the
    * Message Counter being enabled
    * @param queue the queue
    * @return number of messages
    * @throws Exception if a problem occurs
    */
   int getMessageCountForQueue(String queue) throws Exception;

   /**
    * returns how many messagesd have been delivered from a specific queue
    * @param queue the queue
    * @return number of messages
    * @throws Exception if a problem occurs
    */
   int getDeliveringCountForQueue(String queue) throws Exception;

   /**
    * returns how many messages are scheduled for a specific queue
    * @param queue the queue
    * @return number of messages
    * @throws Exception if a problem occurs
    */
   int getScheduledMessageCountForQueue(String queue) throws Exception;

   /**
    * returns the message counter for a queue
    * @param queue the queue
    * @return number of messages
    * @throws Exception if a problem occurs
    */
   MessageCounter getMessageCounterForQueue(String queue) throws Exception;

   /**
    * returns the message statistics for a queue
    * @param queue the queue
    * @return the message statistics
    * @throws Exception if a problem occurs
    */
   MessageStatistics getMessageStatisticsForQueue(String queue) throws Exception;

   /**
    * returns how many consumers a specific queue has
    * @param queue the queue
    * @return number of consumers
    * @throws Exception if a problem occurs
    */
   int getConsumerCountForQueue(String queue) throws Exception;

   /**
    * restes the message counter for a specific queue
    * @param queue the queue
    * @throws Exception if a problem occurs
    */
   void resetMessageCounterForQueue(String queue) throws Exception;

   /**
    * resets the counter history for a specific queue
    * @param queue the queue
    * @throws Exception if a problem occurs
    */
   void resetMessageCounterHistoryForQueue(String queue) throws Exception;

   /**
    * lists all messages for a specific queue. This will contain the message references only
    * @param queue the queue
    * @return the messages
    * @throws Exception if a problem occurs
    */
   List listAllMessagesForQueue(String queue) throws Exception;

   /**
    * lists all messages that match the given selector.
    * @param queue the queue
    * @param selector
    * @return the messages
    * @throws Exception if a problem occurs
    */
   List listAllMessagesForQueue(String queue,String selector) throws Exception;

   /**
    * list all the durable messages for a specific queue
    * @param queue the queue
    * @return the messages
    * @throws Exception if a problem occurs
    */
   List listDurableMessagesForQueue(String queue) throws Exception;

   /**
    * list all the durable messages for a queue that match a given selector
    * @param queue the queue
    * @param selector
    * @return the messages
    * @throws Exception if a problem occurs
    */
   List listDurableMessagesForQueue(String queue,String selector) throws Exception;

   /**
    * lists all the non durable messages for a specific queue.
    * @param queue the queue
    * @return the messages
    * @throws Exception if a problem occurs
    */
   List listNonDurableMessagesForQueue(String queue) throws Exception;

   /**
    * lists all noon durable messages for a queue.
    * @param queue
    * @param selector
    * @return
    * @throws Exception
    */
   List listNonDurableMessagesForQueue(String queue,String selector) throws Exception;

   /**
    * lists all durable messages for a specific queue that match a specific selector
    * @param queue
    * @return
    * @throws Exception
    */
   String listMessageCounterAsHTMLForQueue(String queue) throws Exception;

   /**
    * list the message count history for a specific queue in HTML format
    * @param queue
    * @return
    * @throws Exception
    */
   String listMessageCounterHistoryAsHTMLForQueue(String queue) throws Exception;

   //topic

   /**
    * counts messages received for a topic
    * @param topicName
    * @return
    * @throws Exception
    */
   int getAllMessageCountForTopic(String topicName) throws Exception;

   /**
    * counts durable messages recieved for a topic
    * @param topicName
    * @return
    * @throws Exception
    */
   int getDurableMessageCountForTopic(String topicName) throws Exception;

   /**
    * counts non durable messages recieved for a topic
    * @param topicName
    * @return
    * @throws Exception
    */
   int getNonDurableMessageCountForTopic(String topicName) throws Exception;

   /**
    * counts all subscriptions for a topic
    * @param topicName
    * @return
    * @throws Exception
    */
   int getAllSubscriptionsCountForTopic(String topicName) throws Exception;

   /**
    * counts all durable subscriptions for a topic
    * @param topicName
    * @return
    * @throws Exception
    */
   int getDurableSubscriptionsCountForTopic(String topicName) throws Exception;

   /**
    * counts all non durable subscriptions for a topic
    * @param topicName
    * @return
    * @throws Exception
    */
   int getNonDurableSubscriptionsCountForTopic(String topicName) throws Exception;

   /**
    * removes all the messages for a specific topic
    * @param topic
    * @throws Exception
    * @throws Throwable
    */
   void removeAllMessagesForTopic(String topic) throws Exception, Throwable;

   /**
    * lists all subscriptions for a topic
    * @param topic
    * @return
    * @throws Exception
    */
   List listAllSubscriptionsForTopic(String topic) throws Exception;

   /**
    * lists all durable subscriptions for a topic
    * @param topic
    * @return
    * @throws Exception
    */
   List listDurableSubscriptionsForTopic(String topic) throws Exception;

   /**
    * lists all non durable subscriptions for a topic
    * @param topic
    * @return
    * @throws Exception
    */
   List listNonDurableSubscriptionsForTopic(String topic) throws Exception;

   /**
    * lists all subscriptions for a topic as html
    * @param topic
    * @return
    * @throws Exception
    */
   String listAllSubscriptionsAsHTMLForTopic(String topic) throws Exception;

   /**
    * lists all durable subscriptions for a topic as html
    * @param topic
    * @return
    * @throws Exception
    */
   String listDurableSubscriptionsAsHTMLForTopic(String topic) throws Exception;

   /**
    * lists all non durable subscriptions for a topic as html
    * @param topic
    * @return
    * @throws Exception
    */
   String listNonDurableSubscriptionsAsHTMLForTopic(String topic) throws Exception;

   /**
    * lists all the messages for a topic for a given subscription
    * @param topic
    * @param subscriptionId
    * @return
    * @throws Exception
    */
   List listAllMessagesForTopic(String topic,String subscriptionId) throws Exception;

   /**
    * lists all the messages for a topic for a given subscription and message selector
    * @param topic
    * @param subscriptionId
    * @param selector
    * @return
    * @throws Exception
    */
   List listAllMessagesForTopic(String topic,String subscriptionId, String selector) throws Exception;

   /**
    *  lists durable messages for a topic for a given subscription
    * @param topic
    * @param subscriptionId
    * @return
    * @throws Exception
    */
   List listDurableMessagesForTopic(String topic,String subscriptionId) throws Exception;

   /**
    * lists durable messages for a topic for a given subscription and message selector
    * @param topic
    * @param subscriptionId
    * @param selector
    * @return
    * @throws Exception
    */
   List listDurableMessagesForTopic(String topic,String subscriptionId, String selector) throws Exception;

   /**
    *  lists non durable messages for a topic for a given subscription
    * @param topic
    * @param subscriptionId
    * @return
    * @throws Exception
    */
   List listNonDurableMessagesForTopic(String topic,String subscriptionId) throws Exception;

   /**
    * lists durable messages for a topic for a given subscription and message selector
    * @param topic
    * @param subscriptionId
    * @param selector
    * @return
    * @throws Exception
    */
   List listNonDurableMessagesForTopic(String topic,String subscriptionId, String selector) throws Exception;

   List getMessageCountersForTopic(String topic) throws Exception;
   
   String showActiveClientsAsHTML() throws Exception;

   String showPreparedTransactionsAsHTML();

   String listMessageCountersAsHTML() throws Exception;
}

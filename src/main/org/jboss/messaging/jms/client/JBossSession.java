/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.jms.client;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.TransactionInProgressException;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;
import javax.transaction.xa.XAResource;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTemporaryQueue;
import org.jboss.messaging.jms.JBossTemporaryTopic;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.utils.SimpleString;

/**
 *
 * Note that we *do not* support JMS ASF (Application Server Facilities) optional
 * constructs such as ConnectionConsumer
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossSession implements Session, XASession, QueueSession, XAQueueSession, TopicSession, XATopicSession
{
   // Constants -----------------------------------------------------

   public static final int TYPE_GENERIC_SESSION = 0;

   public static final int TYPE_QUEUE_SESSION = 1;

   public static final int TYPE_TOPIC_SESSION = 2;

   public static final int SERVER_ACKNOWLEDGE = 4;

   private static SimpleString REJECTING_FILTER = new SimpleString("_JBMX=-1");

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossSession.class);

   // Attributes ----------------------------------------------------

   private final JBossConnection connection;

   private final ClientSession session;

   private final int sessionType;

   private final int ackMode;

   private final boolean transacted;

   private final boolean xa;

   private boolean recoverCalled;

   private final Set<JBossMessageConsumer> consumers = new HashSet<JBossMessageConsumer>();

   // Constructors --------------------------------------------------

   public JBossSession(final JBossConnection connection,
                       final boolean transacted,
                       final boolean xa,
                       final int ackMode,
                       final ClientSession session,
                       final int sessionType)
   {
      this.connection = connection;

      this.ackMode = ackMode;

      this.session = session;

      this.sessionType = sessionType;

      this.transacted = transacted;

      this.xa = xa;
   }

   // Session implementation ----------------------------------------

   public BytesMessage createBytesMessage() throws JMSException
   {
      checkClosed();

      return new JBossBytesMessage(session);
   }

   public MapMessage createMapMessage() throws JMSException
   {
      checkClosed();

      return new JBossMapMessage(session);
   }

   public Message createMessage() throws JMSException
   {
      checkClosed();

      return new JBossMessage(session);
   }

   public ObjectMessage createObjectMessage() throws JMSException
   {
      checkClosed();

      return new JBossObjectMessage(session);
   }

   public ObjectMessage createObjectMessage(final Serializable object) throws JMSException
   {
      checkClosed();

      JBossObjectMessage jbm = new JBossObjectMessage(session);

      jbm.setObject(object);

      return jbm;
   }

   public StreamMessage createStreamMessage() throws JMSException
   {
      checkClosed();

      return new JBossStreamMessage(session);
   }

   public TextMessage createTextMessage() throws JMSException
   {
      checkClosed();

      return new JBossTextMessage(session);
   }

   public TextMessage createTextMessage(final String text) throws JMSException
   {
      checkClosed();

      JBossTextMessage jbm = new JBossTextMessage(session);

      jbm.setText(text);

      return jbm;
   }

   public boolean getTransacted() throws JMSException
   {
      checkClosed();

      return transacted;
   }

   public int getAcknowledgeMode() throws JMSException
   {
      checkClosed();

      return ackMode;
   }

   public void commit() throws JMSException
   {
      if (!transacted)
      {
         throw new IllegalStateException("Cannot commit a non-transacted session");
      }
      if (xa)
      {
         throw new TransactionInProgressException("Cannot call commit on an XA session");
      }
      try
      {
         session.commit();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void rollback() throws JMSException
   {
      if (!transacted)
      {
         throw new IllegalStateException("Cannot rollback a non-transacted session");
      }
      if (xa)
      {
         throw new TransactionInProgressException("Cannot call rollback on an XA session");
      }

      try
      {
         session.rollback();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void close() throws JMSException
   {
      try
      {
         for (JBossMessageConsumer cons : new HashSet<JBossMessageConsumer>(consumers))
         {
            cons.close();
         }

         session.close();

         connection.removeSession(this);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void recover() throws JMSException
   {
      if (transacted)
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }

      try
      {
         session.rollback();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }

      recoverCalled = true;
   }

   public MessageListener getMessageListener() throws JMSException
   {
      checkClosed();

      return null;
   }

   public void setMessageListener(final MessageListener listener) throws JMSException
   {
      checkClosed();
   }

   public void run()
   {
   }

   public MessageProducer createProducer(final Destination destination) throws JMSException
   {
      if (destination != null && !(destination instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Not a JBoss Destination:" + destination);
      }

      try
      {
         JBossDestination jbd = (JBossDestination)destination;

         if (jbd != null)
         {
            if (jbd instanceof Queue)
            {
               SessionQueueQueryResponseMessage response = session.queueQuery(jbd.getSimpleAddress());
   
               if (!response.isExists())
               {
                  throw new InvalidDestinationException("Queue " + jbd.getName() + " does not exist");
               }
            }
            else
            {
               SessionBindingQueryResponseMessage response = session.bindingQuery(jbd.getSimpleAddress());
   
               if (!response.isExists())
               {
                  throw new InvalidDestinationException("Topic " + jbd.getName() + " does not exist");
               }
            }
         }

         ClientProducer producer = session.createProducer(jbd == null ? null : jbd.getSimpleAddress());

         return new JBossMessageProducer(connection, producer, jbd, session);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public MessageConsumer createConsumer(final Destination destination) throws JMSException
   {
      return createConsumer(destination, null, false);
   }

   public MessageConsumer createConsumer(final Destination destination, final String messageSelector) throws JMSException
   {
      return createConsumer(destination, messageSelector, false);
   }

   public MessageConsumer createConsumer(final Destination destination,
                                         final String messageSelector,
                                         final boolean noLocal) throws JMSException
   {
      if (destination == null)
      {
         throw new InvalidDestinationException("Cannot create a consumer with a null destination");
      }

      if (!(destination instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Not a JBossDestination:" + destination);
      }

      JBossDestination jbdest = (JBossDestination)destination;

      JBossMessageConsumer consumer = createConsumer(jbdest, null, messageSelector, noLocal);

      return consumer;
   }

   public Queue createQueue(final String queueName) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a queue using a TopicSession");
      }

      JBossQueue queue = new JBossQueue(queueName);

      try
      {
         SessionQueueQueryResponseMessage response = session.queueQuery(queue.getSimpleAddress());

         if (!response.isExists())
         {
            throw new JMSException("There is no queue with name " + queueName);
         }
         else
         {
            return queue;
         }
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public Topic createTopic(final String topicName) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a topic on a QueueSession");
      }

      JBossTopic topic = new JBossTopic(topicName);

      try
      {
         SessionBindingQueryResponseMessage response = session.bindingQuery(topic.getSimpleAddress());

         if (!response.isExists())
         {
            throw new JMSException("There is no topic with name " + topicName);
         }
         else
         {
            return topic;
         }
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public TopicSubscriber createDurableSubscriber(final Topic topic, final String name) throws JMSException
   {
      return createDurableSubscriber(topic, name, null, false);
   }

   public TopicSubscriber createDurableSubscriber(final Topic topic,
                                                  final String name,
                                                  String messageSelector,
                                                  final boolean noLocal) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a durable subscriber on a QueueSession");
      }
      if (topic == null)
      {
         throw new InvalidDestinationException("Cannot create a durable subscriber on a null topic");
      }
      if (!(topic instanceof JBossTopic))
      {
         throw new InvalidDestinationException("Not a JBossTopic:" + topic);
      }
      if ("".equals(messageSelector))
      {
         messageSelector = null;
      }

      JBossDestination jbdest = (JBossDestination)topic;

      return createConsumer(jbdest, name, messageSelector, noLocal);
   }

   private JBossMessageConsumer createConsumer(final JBossDestination dest,
                                               final String subscriptionName,
                                               String selectorString,
                                               final boolean noLocal) throws JMSException
   {
      try
      {
         selectorString = "".equals(selectorString) ? null : selectorString;

         if (noLocal)
         {
            connection.setHasNoLocal();

            String filter = JBossConnection.CONNECTION_ID_PROPERTY_NAME.toString() + "<>'" + connection.getUID() + "'";

            if (selectorString != null)
            {
               selectorString += " AND " + filter;
            }
            else
            {
               selectorString = filter;
            }
         }

         SimpleString coreFilterString = null;

         if (selectorString != null)
         {
            coreFilterString = new SimpleString(SelectorTranslator.convertToJBMFilterString(selectorString));
         }

         ClientConsumer consumer;

         SimpleString autoDeleteQueueName = null;

         if (dest instanceof Queue)
         {
            SessionQueueQueryResponseMessage response = session.queueQuery(dest.getSimpleAddress());

            if (!response.isExists())
            {
               throw new InvalidDestinationException("Queue " + dest.getName() + " does not exist");
            }

            consumer = session.createConsumer(dest.getSimpleAddress(), coreFilterString, false);
         }
         else
         {
            SessionBindingQueryResponseMessage response = session.bindingQuery(dest.getSimpleAddress());

            if (!response.isExists())
            {
               throw new InvalidDestinationException("Topic " + dest.getName() + " does not exist");
            }

            SimpleString queueName;

            if (subscriptionName == null)
            {
               // Non durable sub

               queueName = new SimpleString(UUID.randomUUID().toString());

               session.createTemporaryQueue(dest.getSimpleAddress(), queueName, coreFilterString);

               consumer = session.createConsumer(queueName, null, false);

               autoDeleteQueueName = queueName;
            }
            else
            {
               // Durable sub

               if (connection.getClientID() == null)
               {
                  throw new InvalidClientIDException("Cannot create durable subscription - client ID has not been set");
               }

               if (dest.isTemporary())
               {
                  throw new InvalidDestinationException("Cannot create a durable subscription on a temporary topic");
               }

               queueName = new SimpleString(JBossTopic.createQueueNameForDurableSubscription(connection.getClientID(),
                                                                                             subscriptionName));

               SessionQueueQueryResponseMessage subResponse = session.queueQuery(queueName);

               if (!subResponse.isExists())
               {
                  session.createQueue(dest.getSimpleAddress(), queueName, coreFilterString, true);
               }
               else
               {
                  // Already exists
                  if (subResponse.getConsumerCount() > 0)
                  {
                     throw new IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
                  }

                  // From javax.jms.Session Javadoc (and also JMS 1.1 6.11.1):
                  // A client can change an existing durable subscription by
                  // creating a durable
                  // TopicSubscriber with the same name and a new topic and/or
                  // message selector.
                  // Changing a durable subscriber is equivalent to
                  // unsubscribing (deleting) the old
                  // one and creating a new one.

                  SimpleString oldFilterString = subResponse.getFilterString();

                  boolean selectorChanged = (coreFilterString == null && oldFilterString != null) || (oldFilterString == null && coreFilterString != null) ||
                                            (oldFilterString != null && coreFilterString != null && !oldFilterString.equals(coreFilterString));

                  SimpleString oldTopicName = subResponse.getAddress();

                  boolean topicChanged = !oldTopicName.equals(dest.getSimpleAddress());

                  if (selectorChanged || topicChanged)
                  {
                     // Delete the old durable sub
                     session.deleteQueue(queueName);

                     // Create the new one
                     session.createQueue(dest.getSimpleAddress(), queueName, coreFilterString, true);
                  }
               }

               consumer = session.createConsumer(queueName, null, false);
            }
         }

         JBossMessageConsumer jbc = new JBossMessageConsumer(this,
                                                             consumer,
                                                             noLocal,
                                                             dest,
                                                             selectorString,
                                                             autoDeleteQueueName);

         consumers.add(jbc);

         return jbc;
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public QueueBrowser createBrowser(final Queue queue) throws JMSException
   {
      return createBrowser(queue, null);
   }

   public QueueBrowser createBrowser(final Queue queue, String filterString) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a browser on a TopicSession");
      }
      if (queue == null)
      {
         throw new InvalidDestinationException("Cannot create a browser with a null queue");
      }
      if (!(queue instanceof JBossQueue))
      {
         throw new InvalidDestinationException("Not a JBossQueue:" + queue);
      }
      if ("".equals(filterString))
      {
         filterString = null;
      }

      // eager test of the filter syntax as required by JMS spec
      try
      {
         FilterImpl.createFilter(filterString);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }

      JBossQueue jbq = (JBossQueue)queue;

      try
      {
         SessionBindingQueryResponseMessage message = session.bindingQuery(new SimpleString(jbq.getAddress()));
         if (!message.isExists())
         {
            throw new InvalidDestinationException(jbq.getAddress() + " does not exist");
         }
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }

      return new JBossQueueBrowser(jbq, filterString, session);

   }

   public TemporaryQueue createTemporaryQueue() throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a temporary queue using a TopicSession");
      }

      String queueName = UUID.randomUUID().toString();

      try
      {
         JBossTemporaryQueue queue = new JBossTemporaryQueue(this, queueName);

         SimpleString simpleAddress = queue.getSimpleAddress();

         session.createTemporaryQueue(simpleAddress, simpleAddress);

         connection.addTemporaryQueue(simpleAddress);

         return queue;
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public TemporaryTopic createTemporaryTopic() throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a temporary topic on a QueueSession");
      }

      String topicName = UUID.randomUUID().toString();

      try
      {
         JBossTemporaryTopic topic = new JBossTemporaryTopic(this, topicName);

         SimpleString simpleAddress = topic.getSimpleAddress();

         // We create a dummy subscription on the topic, that never receives messages - this is so we can perform JMS
         // checks when routing messages to a topic that
         // does not exist - otherwise we would not be able to distinguish from a non existent topic and one with no
         // subscriptions - core has no notion of a topic

         session.createQueue(simpleAddress, simpleAddress, REJECTING_FILTER, false);

         connection.addTemporaryQueue(simpleAddress);

         return topic;
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void unsubscribe(final String name) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot unsubscribe using a QueueSession");
      }

      SimpleString queueName = new SimpleString(JBossTopic.createQueueNameForDurableSubscription(connection.getClientID(),
                                                                                                 name));

      try
      {
         SessionQueueQueryResponseMessage response = session.queueQuery(queueName);

         if (!response.isExists())
         {
            throw new InvalidDestinationException("Cannot unsubscribe, subscription with name " + name +
                                                  " does not exist");
         }

         if (response.getConsumerCount() != 0)
         {
            throw new IllegalStateException("Cannot unsubscribe durable subscription " + name +
                                            " since it has active subscribers");
         }

         session.deleteQueue(queueName);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   // XASession implementation

   public Session getSession() throws JMSException
   {
      if (!xa)
      {
         throw new IllegalStateException("Isn't an XASession");
      }

      return this;
   }

   public XAResource getXAResource()
   {
      return session.getXAResource();
   }

   // QueueSession implementation

   public QueueReceiver createReceiver(final Queue queue, final String messageSelector) throws JMSException
   {
      return (QueueReceiver)createConsumer(queue, messageSelector);
   }

   public QueueReceiver createReceiver(final Queue queue) throws JMSException
   {
      return (QueueReceiver)createConsumer(queue);
   }

   public QueueSender createSender(final Queue queue) throws JMSException
   {
      return (QueueSender)createProducer(queue);
   }

   // XAQueueSession implementation

   public QueueSession getQueueSession() throws JMSException
   {
      return (QueueSession)getSession();
   }

   // TopicSession implementation

   public TopicPublisher createPublisher(final Topic topic) throws JMSException
   {
      return (TopicPublisher)createProducer(topic);
   }

   public TopicSubscriber createSubscriber(final Topic topic, final String messageSelector, final boolean noLocal) throws JMSException
   {
      return (TopicSubscriber)createConsumer(topic, messageSelector, noLocal);
   }

   public TopicSubscriber createSubscriber(final Topic topic) throws JMSException
   {
      return (TopicSubscriber)createConsumer(topic);
   }

   // XATopicSession implementation

   public TopicSession getTopicSession() throws JMSException
   {
      return (TopicSession)getSession();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "JBossSession->" + session;
   }

   public ClientSession getCoreSession()
   {
      return session;
   }

   public boolean isRecoverCalled()
   {
      return recoverCalled;
   }

   public void setRecoverCalled(final boolean recoverCalled)
   {
      this.recoverCalled = recoverCalled;
   }

   public void deleteTemporaryTopic(final JBossTemporaryTopic tempTopic) throws JMSException
   {
      try
      {
         SessionBindingQueryResponseMessage response = session.bindingQuery(tempTopic.getSimpleAddress());

         if (!response.isExists())
         {
            throw new InvalidDestinationException("Cannot delete temporary topic " + tempTopic.getName() +
                                                  " does not exist");
         }

         if (response.getQueueNames().size() > 1)
         {
            throw new IllegalStateException("Cannot delete temporary topic " + tempTopic.getName() +
                                            " since it has subscribers");
         }

         SimpleString address = tempTopic.getSimpleAddress();

         session.deleteQueue(address);

         connection.removeTemporaryQueue(address);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void deleteTemporaryQueue(final JBossTemporaryQueue tempQueue) throws JMSException
   {
      try
      {
         SessionQueueQueryResponseMessage response = session.queueQuery(tempQueue.getSimpleAddress());

         if (!response.isExists())
         {
            throw new InvalidDestinationException("Cannot delete temporary queue " + tempQueue.getName() +
                                                  " does not exist");
         }

         if (response.getConsumerCount() > 0)
         {
            throw new IllegalStateException("Cannot delete temporary queue " + tempQueue.getName() +
                                            " since it has subscribers");
         }

         SimpleString address = tempQueue.getSimpleAddress();

         session.deleteQueue(address);

         connection.removeTemporaryQueue(address);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void deleteQueue(final SimpleString queueName) throws JMSException
   {
      try
      {
         session.deleteQueue(queueName);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void start() throws JMSException
   {
      try
      {
         session.start();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void stop() throws JMSException
   {
      try
      {
         session.stop();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public void removeConsumer(final JBossMessageConsumer consumer)
   {
      consumers.remove(consumer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkClosed() throws JMSException
   {
      if (session.isClosed())
      {
         throw new IllegalStateException("Session is closed");
      }
   }

   // Inner classes -------------------------------------------------

}

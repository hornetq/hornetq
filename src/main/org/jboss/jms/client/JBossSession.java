/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.delegate.ProducerDelegate;

import javax.jms.Session;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.jms.QueueBrowser;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossSession implements Session
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected SessionDelegate sessionDelegate;

   // Constructors --------------------------------------------------

   public JBossSession(SessionDelegate delegate)
   {
      this.sessionDelegate = delegate;
   }

   // Session implementation ----------------------------------------

   public BytesMessage createBytesMessage() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public MapMessage createMapMessage() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public Message createMessage() throws JMSException
   {
      return sessionDelegate.createMessage();
   }

   public ObjectMessage createObjectMessage() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public ObjectMessage createObjectMessage(Serializable object) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public StreamMessage createStreamMessage() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public TextMessage createTextMessage() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public TextMessage createTextMessage(String text) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public boolean getTransacted() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public int getAcknowledgeMode() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void commit() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void rollback() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void close() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void recover() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public MessageListener getMessageListener() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void run()
   {
      throw new NotYetImplementedException();
   }

   public MessageProducer createProducer(Destination d) throws JMSException
   {
      ProducerDelegate producerDelegate = sessionDelegate.createProducerDelegate(d);
      return new JBossMessageProducer(producerDelegate);
   }

  public MessageConsumer createConsumer(Destination d) throws JMSException
  {
     return sessionDelegate.createConsumer(d);
  }

  public MessageConsumer createConsumer(Destination destination, String messageSelector)
        throws JMSException
  {
     throw new NotYetImplementedException();
  }

   public MessageConsumer createConsumer(Destination destination,
                                         String messageSelector,
                                         boolean NoLocal)
         throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public Queue createQueue(String queueName) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public Topic createTopic(String topicName) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public TopicSubscriber createDurableSubscriber(Topic topic,
                                                  String name,
                                                  String messageSelector,
                                                  boolean noLocal) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public QueueBrowser createBrowser(Queue queue) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public TemporaryQueue createTemporaryQueue() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public TemporaryTopic createTemporaryTopic() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void unsubscribe(String name) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;


import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Queue;
import javax.jms.Topic;

import org.jboss.jms.client.Closeable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public interface SessionDelegate extends Closeable
{
   public ProducerDelegate createProducerDelegate(Destination destination) throws JMSException;

   public ConsumerDelegate createConsumerDelegate(Destination destination, String selector,
                                                  boolean noLocal, String subscriptionName)
         throws JMSException;

   public Message createMessage() throws JMSException;
   
   public BytesMessage createBytesMessage() throws JMSException;
  
   public MapMessage createMapMessage() throws JMSException;

   public ObjectMessage createObjectMessage() throws JMSException;

   public ObjectMessage createObjectMessage(Serializable object) throws JMSException;

   public StreamMessage createStreamMessage() throws JMSException;

   public TextMessage createTextMessage() throws JMSException;
   
   public TextMessage createTextMessage(String text) throws JMSException;

   /**
    * Creates a queue identity given a Queue name. Does NOT create the physical queue. The physical
    * creation of queues is an administrative task and is not to be initiated by the JMS API, with
    * the exception of temporary queues.
    */
   public Queue createQueue(String queueName) throws JMSException;

   /**
    * Creates a topic identity given a Queue name. Does NOT create the physical topic. The physical
    * creation of topics is an administrative task and is not to be initiated by the JMS API, with
    * the exception of temporary topics.
    */
   public Topic createTopic(String topicName) throws JMSException;

   public void commit() throws JMSException;
   
   public void rollback() throws JMSException;
   
   public void recover() throws JMSException;

   public BrowserDelegate createBrowserDelegate(Destination queue, String messageSelector)
         throws JMSException;

   /**
    * Used to notify the session that a message was delivered on the client. The acknowledgment
    * might not be immediate, though. The session decides when to acknowledge based on the
    * acknowledgment mode.
    */
	public void delivered(String messageID, String receiverID)
         throws JMSException;

   /**
    * Acknowledges a message back to the server (transactionally or not).
    */
	public void acknowledge(String messageID, String receiverID)
         throws JMSException;

   /**
    * Acknowledges all messages received so far by this session.
    */
	public void acknowledgeSession() throws JMSException;

   /**
    * Tell the server to redeliver any un-acked messages.	
    */
	public void redeliver() throws JMSException;
}

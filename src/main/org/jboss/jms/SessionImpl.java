/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class SessionImpl implements Session
{
    protected int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
    private boolean closed = false; // TODO: make sure this is the default.
    private MessageListener messageListener = null;
    private boolean transacted = false;
    // TODO: Might be able to eliminate the seperate lists by implementing a
    //interface which just does a close() if that is all we're uisng this for...
    private List messageConsumers = new ArrayList();
    private List messageProducers = new ArrayList();
    private List queueBrowsers = new ArrayList();

    SessionImpl(boolean transacted, int acknowledgeMode)
    {
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
    }

    public void close() throws JMSException
    {
    }

    public void commit() throws JMSException
    {
        this.throwExceptionIfClosed();
    }

    public QueueBrowser createBrowser(Queue queue) throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.createBrowser(queue, null);
    }

    public QueueBrowser createBrowser(Queue queue, String messageSelector)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        return null;
    }

    public BytesMessage createBytesMessage() throws JMSException
    {
        this.throwExceptionIfClosed();
        return new BytesMessageImpl();
    }

    public MessageConsumer createConsumer(Destination destination)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.createConsumer(destination, null);
    }

    public MessageConsumer createConsumer(
            Destination destination,
            String messageSelector)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.createConsumer(destination, messageSelector, false);
    }

    public MessageConsumer createConsumer(
            Destination destination,
            String messageSelector,
            boolean noLocal)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        MessageConsumer messageConsumer =
                new MessageConsumerImpl(destination, messageSelector, noLocal);
        this.messageConsumers.add(messageConsumer);
        return messageConsumer;
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String name)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.createDurableSubscriber(topic, name, null, false);
    }

    public TopicSubscriber createDurableSubscriber(
            Topic topic,
            String name,
            String messageSelector,
            boolean noLocal)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        return null;
    }

    public MapMessage createMapMessage() throws JMSException
    {
        this.throwExceptionIfClosed();
        return new MapMessageImpl();
    }

    public Message createMessage() throws JMSException
    {
        this.throwExceptionIfClosed();
        return new MessageImpl();
    }

    public ObjectMessage createObjectMessage() throws JMSException
    {
        this.throwExceptionIfClosed();
        return new ObjectMessageImpl();
    }

    public ObjectMessage createObjectMessage(Serializable object)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        return new ObjectMessageImpl(object);
    }

    public MessageProducer createProducer(Destination destination)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        MessageProducer messageProducer = new MessageProducerImpl(destination);
        this.messageProducers.add(messageProducer);
        return messageProducer;
    }

    public Queue createQueue(String queueName) throws JMSException
    {
        this.throwExceptionIfClosed();
        return new DestinationImpl(queueName);
    }

    public StreamMessage createStreamMessage() throws JMSException
    {
        this.throwExceptionIfClosed();
        return new StreamMessageImpl();
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException
    {
        this.throwExceptionIfClosed();
        return null;
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException
    {
        this.throwExceptionIfClosed();
        return null;
    }

    public TextMessage createTextMessage() throws JMSException
    {
        this.throwExceptionIfClosed();
        return new TextMessageImpl();
    }

    public TextMessage createTextMessage(String text) throws JMSException
    {
        this.throwExceptionIfClosed();
        return new TextMessageImpl(text);
    }

    public Topic createTopic(String topicName) throws JMSException
    {
        this.throwExceptionIfClosed();
        return new DestinationImpl(topicName);
    }

    public int getAcknowledgeMode() throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.acknowledgeMode;
    }

    public MessageListener getMessageListener() throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.messageListener;
    }

    public boolean getTransacted() throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.transacted;
    }

    public void recover() throws JMSException
    {
        this.throwExceptionIfClosed();
    }

    public void rollback() throws JMSException
    {
        this.throwExceptionIfClosed();
    }

    public void run()
    {
    }

    public void setMessageListener(MessageListener messageListener)
            throws JMSException
    {
        this.throwExceptionIfClosed();
        this.messageListener = messageListener;
    }

    private void throwExceptionIfClosed() throws IllegalStateException
    {
        if (this.closed)
        {
            throw new IllegalStateException("The session is closed.");
        }
    }

    public void unsubscribe(String name) throws JMSException
    {
        this.throwExceptionIfClosed();
    }

}
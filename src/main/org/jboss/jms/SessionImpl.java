/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;

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

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class SessionImpl implements Session
{
    private ConnectionImpl connection = null;
    private int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
    private boolean closed = false; // TOD: make sure this is the default.
    private MessageListener messageListener = null;
    private boolean transacted = false;
    // TOD: Might be able to eliminate the seperate lists by implementing a
    //interface which just does a close() if that is all we're uisng this for...
    private List messageConsumers = new ArrayList();
    private List messageProducers = new ArrayList();
    private List queueBrowsers = new ArrayList();

    private Map unacknowledgedMessageMap = new TreeMap();
    private long nextDeliveryId = 0;
    private boolean recovering = false;
    private Object recoveryLock = new Object();
    private List uncommittedMessages = new ArrayList();

    SessionImpl(ConnectionImpl connection, boolean transacted, int acknowledgeMode)
    {
        this.connection = connection;
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
    }

    public void close() throws JMSException
    {
        if (!this.closed)
        {
            if (this.transacted)
            {
                this.rollback();
            }
            Iterator iterator = this.messageConsumers.iterator();
            while (iterator.hasNext())
            {
                ((MessageConsumer) iterator.next()).close();
                iterator.remove();
            }
            iterator = this.messageProducers.iterator();
            while (iterator.hasNext())
            {
                ((MessageProducer) iterator.next()).close();
            }
            iterator = this.queueBrowsers.iterator();
            while (iterator.hasNext())
            {
                ((QueueBrowser) iterator.next()).close();
            }
            this.closed = true;
        }
    }

    public void commit() throws JMSException
    {
        this.throwExceptionIfClosed();
        if (this.transacted)
        {
            this.recovering = true;
            if (this.uncommittedMessages.size() > 0)
            {
                this.connection.send((Collection) ((ArrayList) this.uncommittedMessages).clone());
            }
            this.unacknowledgedMessageMap.clear();
            this.uncommittedMessages.clear();
            this.recovering = false;
            synchronized (this.recoveryLock)
            {
                this.recoveryLock.notify();
            }
        }
        else
        {
            throw new IllegalStateException("Illegal Operation: This is not a transacted Session.");
        }
    }

    public QueueBrowser createBrowser(Queue queue) throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.createBrowser(queue, null);
    }

    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException
    {
        this.throwExceptionIfClosed();
        return new QueueBrowserImpl(this, queue, messageSelector);
    }

    public BytesMessage createBytesMessage() throws JMSException
    {
        this.throwExceptionIfClosed();
        return new BytesMessageImpl();
    }

    public MessageConsumer createConsumer(Destination destination) throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.createConsumer(destination, null);
    }

    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.createConsumer(destination, messageSelector, false);
    }

    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException
    {
        this.throwExceptionIfClosed();
        MessageConsumer messageConsumer = new MessageConsumerImpl(this, destination, messageSelector, noLocal);
        this.messageConsumers.add(messageConsumer);
        return messageConsumer;
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.createDurableSubscriber(topic, name, null, false);
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException
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

    public ObjectMessage createObjectMessage(Serializable object) throws JMSException
    {
        this.throwExceptionIfClosed();
        return new ObjectMessageImpl(object);
    }

    public MessageProducer createProducer(Destination destination) throws JMSException
    {
        this.throwExceptionIfClosed();
        MessageProducer messageProducer = new MessageProducerImpl(this, destination);
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
        return this.connection.createTemporaryDestination();
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException
    {
        this.throwExceptionIfClosed();
        return this.connection.createTemporaryDestination();
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
        if (this.transacted)
        {
            throw new IllegalStateException("Illegal Operation: This is a transacted Session.  Use rollback instead.");
        }
        synchronized (this.unacknowledgedMessageMap)
        {
            this.recovering = true;
            Map clone = (Map) ((TreeMap) this.unacknowledgedMessageMap).clone();
            this.unacknowledgedMessageMap.clear();
            this.restart(clone);
        }
    }

    public void rollback() throws JMSException
    {
        this.throwExceptionIfClosed();
        if (this.transacted)
        {
            synchronized (this.unacknowledgedMessageMap)
            {
                this.recovering = true;
                Map clone = (Map) ((TreeMap) this.unacknowledgedMessageMap).clone();
                this.unacknowledgedMessageMap.clear();
                this.restart(clone);
            }
            this.uncommittedMessages.clear();
        }
        else
        {
            throw new IllegalStateException("Illegal Operation: This is not a transacted Session.");
        }
    }

    public void run()
    {
    }

    public void setMessageListener(MessageListener messageListener) throws JMSException
    {
        this.throwExceptionIfClosed();
        this.messageListener = messageListener;
    }

    public void unsubscribe(String name) throws JMSException
    {
        this.throwExceptionIfClosed();
    }

    private void throwExceptionIfClosed() throws IllegalStateException
    {
        if (this.closed)
        {
            throw new IllegalStateException("The session is closed.");
        }
    }

    private void restart(final Map unacknowledgedMessage)
    {
        Thread thread = new Thread(new Runnable()
        {
            public void run()
            {
                Iterator iterator = unacknowledgedMessage.keySet().iterator();
                while (iterator.hasNext())
                {
                    MessageImpl message = (MessageImpl) unacknowledgedMessage.get(iterator.next());
                    message.setJMSRedelivered(true);
                    deliver(message, true);
                }
                recovering = false;
                synchronized (recoveryLock)
                {
                    recoveryLock.notify();
                }
            }
        });
        thread.start();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Package protected methods used by objects created by this                                 //
    ///////////////////////////////////////////////////////////////////////////////////////////////
    synchronized void send(MessageImpl message) throws JMSException
    {
        if (this.transacted)
        {
            this.uncommittedMessages.add(message.clone());
        }
        else
        {
            this.connection.send(message);
        }
    }

    void ancknowledge(long deliveryId)
    {
        if (!this.transacted)
        {
            synchronized (this.unacknowledgedMessageMap)
            {
                Iterator iterator = this.unacknowledgedMessageMap.keySet().iterator();
                while (iterator.hasNext())
                {
                    Long currentKey = (Long) iterator.next();
                    if (currentKey.longValue() <= deliveryId)
                    {
                        iterator.remove();
                    }
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Package protected methods used by the connection                                          //
    ///////////////////////////////////////////////////////////////////////////////////////////////
    void deliver(MessageImpl message)
    {
        this.deliver(message, false);
    }

    private void deliver(MessageImpl message, boolean recoveryOperation)
    {
        if (this.recovering && !recoveryOperation)
        {
            synchronized (this.recoveryLock)
            {
                try
                {
                    this.recoveryLock.wait();
                }
                catch (InterruptedException e)
                {
                }
            }
        }
        //message.setSession(this);
        message.setDeliveryId(++this.nextDeliveryId);
        Iterator iterator = this.messageConsumers.iterator();
        if (this.acknowledgeMode != Session.AUTO_ACKNOWLEDGE)
        {
            synchronized (unacknowledgedMessageMap)
            {
                this.unacknowledgedMessageMap.put(new Long(this.nextDeliveryId), message);
            }
        }
        while (iterator.hasNext())
        {
            ((MessageConsumerImpl) iterator.next()).deliver(message);
        }
    }
}
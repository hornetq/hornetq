/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class MessageProducerImpl implements MessageProducer
{
    private SessionImpl session = null;
    private int deliveryMode = Session.AUTO_ACKNOWLEDGE;
    private Destination destination = null;
    private boolean disableMessageID = false;
    private boolean disableMessageTimestamp = false;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;

    MessageProducerImpl(SessionImpl session, Destination destination)
    {
        this.session = session;
        this.destination = destination;
    }

    public void close() throws JMSException
    {
    }

    public int getDeliveryMode() throws JMSException
    {
        return this.deliveryMode;
    }

    public Destination getDestination() throws JMSException
    {
        return this.destination;
    }

    public boolean getDisableMessageID() throws JMSException
    {
        return this.disableMessageID;
    }

    public boolean getDisableMessageTimestamp() throws JMSException
    {
        return this.disableMessageTimestamp;
    }

    public int getPriority() throws JMSException
    {
        return this.priority;
    }

    public long getTimeToLive() throws JMSException
    {
        return this.timeToLive;
    }

    public void send(Destination destination, Message message) throws JMSException
    {
        this.send(destination, message, this.deliveryMode, this.priority, this.timeToLive);
    }

    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
    {

        MessageImpl clone = (MessageImpl) ((MessageImpl) message).clone();
        clone.setJMSDestination(destination);
        clone.setJMSDeliveryMode(deliveryMode);
        clone.setJMSPriority(priority);
        if (timeToLive != 0)
        {
            clone.setJMSExpiration(System.currentTimeMillis() + timeToLive);
        }
        clone.setReadOnly(true);
        this.session.send(clone);
    }

    public void send(Message message) throws JMSException
    {
        this.send(this.destination, message, this.deliveryMode, this.priority, this.timeToLive);
    }

    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
    {
        this.send(this.destination, message, deliveryMode, priority, timeToLive);
    }

    public void setDeliveryMode(int deliveryMode) throws JMSException
    {
        this.deliveryMode = deliveryMode;
    }

    public void setDisableMessageID(boolean value) throws JMSException
    {
        this.disableMessageID = value;
    }

    public void setDisableMessageTimestamp(boolean value) throws JMSException
    {
        this.disableMessageTimestamp = value;
    }

    public void setPriority(int priority) throws JMSException
    {
        this.priority = priority;
    }

    public void setTimeToLive(long timeToLive) throws JMSException
    {
        this.timeToLive = timeToLive;
    }

}
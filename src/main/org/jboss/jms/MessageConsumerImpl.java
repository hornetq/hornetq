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
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class MessageConsumerImpl implements MessageConsumer
{
    private SessionImpl session = null;
    private Destination destination = null;
    private MessageListener messageListener = null;
    private String messageSelector = null;
    protected boolean noLocal = false;
    private boolean waiting = false;
    private Message lastReceivedMessage = null;

    MessageConsumerImpl(SessionImpl session, Destination destination, String messageSelector, boolean noLocal)
    {
        this.session = session;
        this.destination = destination;
        this.messageSelector = messageSelector;
        this.noLocal = noLocal;
    }

    public void close() throws JMSException
    {
        //TOD: Implement close()
    }

    public MessageListener getMessageListener() throws JMSException
    {
        return this.messageListener;
    }

    public String getMessageSelector() throws JMSException
    {
        return this.messageSelector;
    }

    public Message receive() throws JMSException
    {
        return this.receive(0L);
    }

    public Message receive(long timeout) throws JMSException
    {
        Message message = this.lastReceivedMessage;
        if (message == null)
        {
            this.waiting = true;
            synchronized (this)
            {
                try
                {
                    this.wait(timeout);
                }
                catch (InterruptedException exception){}
            }
            message = this.lastReceivedMessage;
            this.lastReceivedMessage = null;
            this.waiting = false;
        }
        return message;
    }

    public Message receiveNoWait() throws JMSException
    {
        this.waiting = true;
        Message message = this.lastReceivedMessage;
        this.waiting = false;
        this.lastReceivedMessage = null;
        return message;
    }

    public void setMessageListener(MessageListener messageListener) throws JMSException
    {
        this.messageListener = messageListener;
    }

    boolean deliver(MessageImpl message)
    {
        try
        {
            if (this.noLocal && message.isLocal())
            {
                return false;
            }
            if (message.getJMSDestination() != null)
            {
                if (message.getJMSDestination().equals(this.destination))
                {
                    if (this.messageListener != null)
                    {
                        this.messageListener.onMessage((Message)message.clone());
                        return true;
                    }
                    else
                    {
                        if (this.waiting)
                        {
                            this.lastReceivedMessage = (MessageImpl)message.clone();
                            synchronized(this)
                            {
                                this.notify();
                            }
                            return true;
                        }
                    }
                }
            }
            return false;
        }
        catch (Exception e){}
        return false;
    }

}
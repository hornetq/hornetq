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

    private Destination destination = null;
    private MessageListener messageListener = null;
    private String messageSelector = null;
    protected boolean noLocal = false;

    MessageConsumerImpl(
            Destination destination,
            String messageSelector,
            boolean noLocal)
    {
        this.destination = destination;
        this.messageSelector = messageSelector;
        this.noLocal = noLocal;
    }

    public void close() throws JMSException
    {
        //TODO: Implement close()
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
        return null;
    }

    public Message receive(long timeout) throws JMSException
    {
        return null;
    }

    public Message receiveNoWait() throws JMSException
    {
        return this.receive(0);
    }

    public void setMessageListener(MessageListener messageListener)
            throws JMSException
    {
        this.messageListener = messageListener;
    }

}
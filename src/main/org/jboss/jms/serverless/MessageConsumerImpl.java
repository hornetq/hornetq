/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageConsumer;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
abstract class MessageConsumerImpl implements MessageConsumer {

    private static final Logger log = Logger.getLogger(MessageConsumerImpl.class);

    protected SessionImpl session;
    private MessageListener listener;
    private Destination destination;

    MessageConsumerImpl(SessionImpl session, Destination destination) {

        this.session = session;
        this.destination = destination;
    }

    Destination getDestination() {
        return destination;
    }

    //
    // MessageConsumer INTERFACE IMPLEMENTATION
    //

    public String getMessageSelector() throws JMSException {
        throw new NotImplementedException();
    }

    public MessageListener getMessageListener() throws JMSException {
        return listener;
    }

    public void setMessageListener(MessageListener listener) throws JMSException {
        this.listener = listener;
    }

    public Message receive() throws JMSException {
        throw new NotImplementedException();
    }

    public Message receive(long timeout) throws JMSException {
        throw new NotImplementedException();
    }

    public Message receiveNoWait() throws JMSException {
        throw new NotImplementedException();
    }

    public abstract void close() throws JMSException;

}

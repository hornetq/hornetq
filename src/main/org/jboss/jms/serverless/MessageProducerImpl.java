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
import javax.jms.MessageProducer;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class MessageProducerImpl implements MessageProducer {

    private static final Logger log = Logger.getLogger(MessageProducerImpl.class);

    private SessionImpl session;
    private Destination destination;

    MessageProducerImpl(SessionImpl session, Destination destination) {

        this.session = session;
        this.destination = destination;

        // TO_DO
    }


    //
    // MessageProducer INTERFACE IMPLEMENTATION
    // 


    public void setDisableMessageID(boolean value) throws JMSException {
        throw new NotImplementedException();
    }

    public boolean getDisableMessageID() throws JMSException {
        throw new NotImplementedException();
    }

    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        throw new NotImplementedException();
    }

    public boolean getDisableMessageTimestamp() throws JMSException {
        throw new NotImplementedException();
    }

    public void setDeliveryMode(int deliveryMode) throws JMSException {
        throw new NotImplementedException();
    }

    public int getDeliveryMode() throws JMSException {
        throw new NotImplementedException();
    }

    public void setPriority(int defaultPriority) throws JMSException {
        throw new NotImplementedException();
    }

    public int getPriority() throws JMSException {
        throw new NotImplementedException();
    }

    public void setTimeToLive(long timeToLive) throws JMSException {
        throw new NotImplementedException();
    }
 
    public long getTimeToLive() throws JMSException {
        throw new NotImplementedException();
    }

    public Destination getDestination() throws JMSException {
        return destination;
    }
    
    public void close() throws JMSException {
        throw new NotImplementedException();
    }
    
    public void send(Message message) throws JMSException {

        message.setJMSDestination(destination);
        session.send(message);

    }
    
    public void send(Message message, int deliveryMode, int priority, long timeToLive)
        throws JMSException {
        throw new NotImplementedException();
    }
    
    public void send(Destination destination, Message message) throws JMSException {
        throw new NotImplementedException();
    }
 
    public void send(Destination destination, 
                     Message message, 
                     int deliveryMode, 
                     int priority,
                     long timeToLive) throws JMSException {
        throw new NotImplementedException();
    }
    
}

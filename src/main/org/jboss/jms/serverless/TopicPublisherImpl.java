/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.Message;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class TopicPublisherImpl extends MessageProducerImpl implements TopicPublisher {

    private static final Logger log = Logger.getLogger(TopicPublisherImpl.class);

    TopicPublisherImpl(SessionImpl session, Topic topic) {
        super(session, topic);

        // TO_DO
    }

    //
    // TopicPublisher INTERFACE IMPLEMENTATION
    // 
    
    public Topic getTopic() throws JMSException {
        throw new NotImplementedException();
    }

    public void publish(Message message) throws JMSException {
        throw new NotImplementedException();
    }

    public void publish(Message message, int deliveryMode, int priority, long timeToLive)
        throws JMSException {
        throw new NotImplementedException();
    }

    public void publish(Topic topic, Message message) throws JMSException {
        throw new NotImplementedException();
    }

    public void publish(Topic topic, 
                        Message message, 
                        int deliveryMode, 
                        int priority,
                        long timeToLive) throws JMSException {
        throw new NotImplementedException();
    }

}

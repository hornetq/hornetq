/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.jms.JMSException;
import javax.jms.MessageListener;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class TopicSubscriberImpl extends MessageConsumerImpl implements TopicSubscriber {

    private static final Logger log = Logger.getLogger(TopicSubscriberImpl.class);

    TopicSubscriberImpl(SessionImpl session, Topic topic) {

        super(session, topic);
    }

    //
    // MessageConsumer INTERFACE METHODS
    //

    public void close() throws JMSException {
        throw new NotImplementedException();
    }


    //
    // TopicSubscriber INTERFACE IMPLEMENTATION
    //

    public Topic getTopic() throws JMSException {
        return (Topic)getDestination();
    }

    public boolean getNoLocal() throws JMSException {
        throw new NotImplementedException();
    }
}

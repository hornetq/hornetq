/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.JMSException;
import javax.jms.Destination;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class QueueReceiverImpl extends MessageConsumerImpl implements QueueReceiver {

    private static final Logger log = Logger.getLogger(TopicSubscriberImpl.class);

    private String id;

    /**
     * @param id  - the receiver id. The Session instance that owns this receiver instance
     *        guarantees id uniqueness during its lifetime.
     **/
    QueueReceiverImpl(SessionImpl session, String id, Queue queue) {

        super(session, queue);
        this.id = id;
    }

    public String getID() {
        return id;
    }

    //
    // MessageConsumer INTERFACE METHODS
    //

    public void close() throws JMSException {
        setMessageListener(null);
        session.removeConsumer(this);
    }

    //
    // QueueReceiver INTERFACE IMPLEMENTATION
    //

    public Queue getQueue() throws JMSException {
        return (Queue)getDestination();
    }
    
}

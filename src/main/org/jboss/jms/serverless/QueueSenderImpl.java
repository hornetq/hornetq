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
import javax.jms.QueueSender;
import javax.jms.Queue;
/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class QueueSenderImpl extends MessageProducerImpl implements QueueSender {

    private static final Logger log = Logger.getLogger(QueueSenderImpl.class);

    QueueSenderImpl(SessionImpl session, Queue queue) {
        super(session, queue);

        // TO_DO
    }

    //
    // QueueSender INTERFACE IMPLEMENTATION
    // 
    public Queue getQueue() throws JMSException {
        throw new NotImplementedException();
    }

//     public void send(Message message) throws JMSException {
//         throw new NotImplementedException();
//     }
    
//     public void send(Message message, int deliveryMode, int priority, long timeToLive) 
//         throws JMSException {
//         throw new NotImplementedException();
//     }

    public void send(Queue queue, Message message) throws JMSException {
        throw new NotImplementedException();
    } 

    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive)
        throws JMSException {
        throw new NotImplementedException();
    }

}

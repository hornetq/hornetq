/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import java.io.Serializable;
import javax.jms.Session;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.jms.QueueBrowser;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class SessionImpl implements Session {

    private static final Logger log = Logger.getLogger(SessionImpl.class);

    private SessionManager sessionManager;
    private String id;
    private List subscribers;
    private List receivers;
    private boolean transacted;
    private int acknowledgeMode;
    private int receiverCounter = 0;

    /**
     * @param id - the session id. The SessionManager instance guarantees uniqueness during its
     *        lifetime.
     **/
    SessionImpl(SessionManager sessionManager, 
                String id, 
                boolean transacted,
                int acknowledgeMode) {
        
        this.sessionManager = sessionManager;
        this.id = id;
        subscribers = new ArrayList();
        receivers = new ArrayList();
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;

        if (transacted) {
            throw new NotImplementedException("Transacted sessions not supported");
        }
    }

    public String getID() {
        return id;
    }

    void send(Message m) throws JMSException {
        sessionManager.getConnection().send(m);
    }

    /**
     * Delivery to topic subscribers.
     **/
    // TO_DO: acknowledgement, deal with failed deliveries
    void deliver(Message m) {

        // TO_DO: single threaded access for sessions
        // So far, the only thread that accesses dispatch() is the connection's puller thread and
        // this will be the unique thread that accesses the Sessions. This may not be sufficient
        // for high load, consider the possiblity to (dynamically) add new threads to handle 
        // delivery, possibly a thread per session. 

        Destination destination = null;
        try {
            destination = m.getJMSDestination();
        }
        catch(JMSException e) {
            // TO_DO: cannot deliver, a failure handler should take over
            log.error("Unhandled failure", e);
            return;
        }

        // TO_DO: properly handle the case when the destination is null

        for(Iterator i = subscribers.iterator(); i.hasNext(); ) {

            TopicSubscriberImpl sub = (TopicSubscriberImpl)i.next();
            if (destination.equals(sub.getDestination())) {
                MessageListener l = null;
                try {
                    l = sub.getMessageListener();
                }
                catch(JMSException e) {
                    // TO_DO: cannot deliver, a failure handler should take over
                    log.error("Unhandled failure", e);
                    continue;
                }
                if (l == null) {
                    continue;
                }
                l.onMessage(m);
            }
        }
    }

    /**
     * Delivery to queue receivers.
     **/
    // TO_DO: acknowledgement, deal with failed deliveries
    void deliver(Message m, String receiverID) {

        // TO_DO: single threaded access for sessions
        // So far, the only thread that accesses dispatch() is the connection's puller thread and
        // this will be the unique thread that accesses the Sessions. This may not be sufficient
        // for high load, consider the possiblity to (dynamically) add new threads to handle 
        // delivery, possibly a thread per session. 

        QueueReceiverImpl receiver = null;
        for(Iterator i = receivers.iterator(); i.hasNext(); ) {

            QueueReceiverImpl crtRec = (QueueReceiverImpl)i.next();
            if (crtRec.getID().equals(receiverID)) {
                receiver = crtRec;
                break;
            }
        }

        if (receiver == null) {
            log.error("No such receiver: "+receiverID+". Delivery failed!");
            return;
        }
        MessageListener l = null;
        try {
            l = receiver.getMessageListener();
        }
        catch(JMSException e) {
            // TO_DO: cannot deliver, a failure handler should take over
            log.error("Unhandled failure", e);
            return;
        }
        if (l == null) {
            log.warn("No message listener for receiver "+receiverID+". Delivery failed!");
        }
        else {
            l.onMessage(m);
        }
    }
    

    //
    // Session INTERFACE IMPLEMEMENTATION
    //

    public BytesMessage createBytesMessage() throws JMSException {
        throw new NotImplementedException();
    }

    public MapMessage createMapMessage() throws JMSException {
        throw new NotImplementedException();
    }

    public Message createMessage() throws JMSException {
        throw new NotImplementedException();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        throw new NotImplementedException();
    }

    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        throw new NotImplementedException();
    }

    public StreamMessage createStreamMessage() throws JMSException {
        throw new NotImplementedException();
    }

    public TextMessage createTextMessage() throws JMSException {
        return new TextMessageImpl();
    }

    public TextMessage createTextMessage(String text) throws JMSException {
        throw new NotImplementedException();
    }

    public boolean getTransacted() throws JMSException {
        return transacted;
    }
    
    public int getAcknowledgeMode() throws JMSException {
        return acknowledgeMode;
    }

    public void commit() throws JMSException {
        throw new NotImplementedException();
    }

    public void rollback() throws JMSException {
        throw new NotImplementedException();
    }

    public void close() throws JMSException {
        throw new NotImplementedException();
    }

    public void recover() throws JMSException {
        throw new NotImplementedException();
    }

    public MessageListener getMessageListener() throws JMSException {
        throw new NotImplementedException();
    }

    public void setMessageListener(MessageListener listener) throws JMSException {
        throw new NotImplementedException();
    }

    public void run() {
        throw new NotImplementedException();
    }
    
    public MessageProducer createProducer(Destination destination) throws JMSException {
        
        if (destination instanceof Topic) {
            return new TopicPublisherImpl(this, (Topic)destination);
        }
        else if (destination instanceof Queue) {
            return new QueueSenderImpl(this, (Queue)destination);
        }
        throw new JMSException("Destination not a Topic or Queue");
    }

    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        
        if (destination instanceof Topic) {
            TopicSubscriberImpl ts = new TopicSubscriberImpl(this, (Topic)destination);
            subscribers.add(ts);
            return ts;
        }
        else if (destination instanceof Queue) {
            QueueReceiverImpl qr = 
                new QueueReceiverImpl(this, generateReceiverID(), (Queue)destination);
            sessionManager.advertiseQueueReceiver(getID(), qr, true);
            receivers.add(qr);
            return qr;
        }
        throw new JMSException("Destination not a Topic or Queue");
    }


    public MessageConsumer createConsumer(Destination destination, String messageSelector) 
        throws JMSException {
        throw new NotImplementedException();
    }

    public MessageConsumer createConsumer(Destination destination, 
                                          String messageSelector, 
                                          boolean NoLocal)   
        throws JMSException {
        throw new NotImplementedException();
    }
    
    public Queue createQueue(String queueName) throws JMSException {
        throw new NotImplementedException();
    }
    
    public Topic createTopic(String topicName) throws JMSException {
        throw new NotImplementedException();
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, 
                                                   String name) throws JMSException {
        throw new NotImplementedException();
    }

    public TopicSubscriber createDurableSubscriber(Topic topic,
                                                   String name, 
                                                   String messageSelector,
                                                   boolean noLocal) throws JMSException {
        throw new NotImplementedException();
    }
    
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new NotImplementedException();
    }

    public QueueBrowser createBrowser(Queue queue,
                                      String messageSelector) throws JMSException {
        throw new NotImplementedException();
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new NotImplementedException();
    }
   
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new NotImplementedException();
    }

    public void unsubscribe(String name) throws JMSException {
        throw new NotImplementedException();
    }

    //
    // END Session INTERFACE IMPLEMEMENTATION
    //

    /**
     * The reverse of createConsumer().
     **/
    void removeConsumer(MessageConsumer consumer) throws JMSException {

        if (consumer instanceof QueueReceiverImpl) {
            if (!receivers.contains(consumer)) {
                throw new JMSException("No such QueueReceiver: "+consumer);
            }
            sessionManager.advertiseQueueReceiver(getID(), (QueueReceiverImpl)consumer, false);
            receivers.remove(consumer);
        }
        else if (consumer instanceof TopicSubscriberImpl) {
            throw new NotImplementedException();
        }
        else {
            throw new JMSException("MessageConsumer not a TopicSubscriber or a QueueReceiver");
        }
    }

    
    /**
     * Generate a queue receiver ID that is quaranteed to be unique for the life time of this
     * Session instance. 
     **/
    private synchronized String generateReceiverID() {
        return Integer.toString(receiverCounter++);
    }

    //
    // LIST MANAGEMENT METHODS
    //

    //private QueueReceiverImpl getReceiver(String receiverID)

    //
    // END OF LIST MANAGEMENT METHODS
    //

}

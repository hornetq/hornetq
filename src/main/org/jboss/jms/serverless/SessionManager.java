/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Session;
import java.util.List;
import java.util.ArrayList;
import javax.jms.JMSException;
import java.util.Iterator;
import javax.jms.Message;

/**
 * The main reason for this class to exist is to insure synchronized access to the connection's
 * session list. It also handles message delivery from the group to sessions and vice-versa.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class SessionManager implements Runnable {

    private static final Logger log = Logger.getLogger(SessionManager.class);

    private GroupConnection connection;
    private org.jgroups.util.Queue deliveryQueue;
    private Thread deliveryThread;
    private List sessions;
    private int sessionCounter = 0;

    SessionManager(GroupConnection connection, org.jgroups.util.Queue deliveryQueue) {

        this.connection = connection;
        this.deliveryQueue = deliveryQueue;
        sessions = new ArrayList();
        deliveryThread = new Thread(this, "Session Delivery Thread");
        deliveryThread.start();
    }

    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {

        Session s = new SessionImpl(this, generateSessionID(), transacted, acknowledgeMode);
        synchronized(sessions) {
            sessions.add(s);
        }
        return s;
    }

    /**
     * The only way for the managed Session to access the Connection instance. If a session
     * needs to access the connection directly, that's the way it gets the instance.
     **/
    GroupConnection getConnection() {
        return connection;
    }

    // TO_DO: acknowledgement, deal with failed deliveries
    private void deliver(Message m) {

        // TO_DO: single threaded access for sessions
        // So far, the only thread that accesses dispatch() is the connection's puller thread and
        // this will be the unique thread that accesses the Sessions. This may not be sufficient
        // for high load, consider the possiblity to (dynamically) add new threads to handle 
        // delivery, possibly a thread per session. 

        synchronized(sessions) {
            for(Iterator i = sessions.iterator(); i.hasNext(); ) {
                ((SessionImpl)i.next()).deliver(m);
            }
        }
    }


    // TO_DO: acknowledgement, deal with failed deliveries
    private void deliver(Message m, String sessionID, String queueReceiverID) {


        // TO_DO: single threaded access for sessions
        // So far, the only thread that accesses dispatch() is the connection's puller thread and
        // this will be the unique thread that accesses the Sessions. This may not be sufficient
        // for high load, consider the possiblity to (dynamically) add new threads to handle 
        // delivery, possibly a thread per session. 

        SessionImpl session = null;
        synchronized(sessions) {
            for(Iterator i = sessions.iterator(); i.hasNext(); ) {
                SessionImpl crts = (SessionImpl)i.next();
                if (crts.getID().equals(sessionID)) {
                    session = crts;
                    break;
                }
            }
        }
        if (session == null) {
            log.error("No such session: "+sessionID+". Delivery failed!");
        }
        else {
            session.deliver(m, queueReceiverID);
        }
    }

    /**
     * Method called by a managed sessions when a new queue receiver is created or removed.  
     * The queue receiver has to be advertised to the group, to update the queue section of the
     * group state.
     **/
    void advertiseQueueReceiver(String sessionID, QueueReceiverImpl qr, boolean isOn) 
        throws JMSException {
        try {
            connection.
                advertiseQueueReceiver(qr.getQueue().getQueueName(), sessionID, qr.getID(), isOn);
        }
        catch(ProviderException e) {
            // the multicast failed, the queue receiver is invalid
            String msg = "Cannot advertise queue receiver";
            JMSException jmse = new JMSException(msg);	
            jmse.setLinkedException(e);
            throw jmse;
        }
    }

    //
    //
    //

    /**
     * Generate a session ID that is quaranteed to be unique for the life time of a SessionManager
     * instance.
     **/
    private synchronized String generateSessionID() {
        return Integer.toString(sessionCounter++);
    }

    //
    // Runnable INTERFACE IMPLEMENTATION
    // 

    public void run() {

        while(true) {

            try {
                Object o = deliveryQueue.remove();
                if (o instanceof javax.jms.Message) {
                    deliver((javax.jms.Message)o);
                }
                else if (o instanceof QueueCarrier) {
                    QueueCarrier qc = (QueueCarrier)o;
                    deliver(qc.getJMSMessage(), qc.getSessionID(), qc.getReceiverID());
                }
                else {
                    log.warn("Unknown delivery object: " +
                             (o == null ? "null" : o.getClass().getName()));
                }
            }
            catch(Exception e) {
                log.warn("Failed to remove element from the delivery queue", e);
            }
        }
    }

    //
    // END Runnable INTERFACE IMPLEMENTATION
    // 

}

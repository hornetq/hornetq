/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;

/**
 * An object that holds the current state of a connection. The state could be one, and only one of 
 * DISCONNECTED, STOPPED, STARTED, CLOSED. The instance's lock is used during the operations that 
 * change the connection state.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class ConnectionState {

    private static final Logger log = Logger.getLogger(ConnectionState.class);

    public static final int DISCONNECTED = 0;
    public static final int STOPPED = 1;
    public static final int STARTED = 2;
    public static final int CLOSED = 3;

    private int state;

    public ConnectionState() {
        state = DISCONNECTED;
    }

    public synchronized boolean isDisconnected() {
        return state == DISCONNECTED;
    }

    public synchronized boolean isStopped() {
        return state == STOPPED;
    }

    public synchronized boolean isStarted() {
        return state == STARTED;
    }

    public synchronized boolean isClosed() {
        return state == CLOSED;
    }

    // No state consistency check is performed at this level. State changing methods should do
    // that and throw apropriate exceptions.

    public synchronized void setStopped() {
        state = STOPPED;
    }

    public synchronized void setStarted() {
        state = STARTED;
    }

    public synchronized void setClosed() {
        state = CLOSED;
    }

    public static String stateToString(ConnectionState cs) {
        return 
            cs.state == DISCONNECTED ? "DISCONNECTED" :
            cs.state == STOPPED ? "STOPPED" :
            cs.state == STARTED ? "STARTED" :
            cs.state == CLOSED ? "CLOSED" : "UNKNOWN";
    }
   

}

/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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

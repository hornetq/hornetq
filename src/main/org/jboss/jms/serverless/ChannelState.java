/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;

/**
 * An object whose lock is used to control the Connection Management Thread. Has a binary state
 * (open/not open).
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class ChannelState {

    private static final Logger log = Logger.getLogger(ChannelState.class);

    private boolean open;

    public ChannelState() {
        open = false;
    }

    public synchronized boolean isOpen() {
        return open;
    }

    public synchronized boolean isNotOpen() {
        return !open;
    }

    public synchronized void setOpen(boolean b) {
        open = b;
    }

}

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
import org.jgroups.Address;
import java.io.Serializable;

/**
 * A wrapper around information that uniquely identifies a QueueReceiver in a group.
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class QueueReceiverAddress implements Serializable {

    static final long serialVersionUID = 11480310721131223L;

    private static final Logger log = Logger.getLogger(QueueReceiverAddress.class);

    private Address addr;
    private String sessionID;
    private String queueReceiverID;
    private boolean nextForDelivery;

    public QueueReceiverAddress(Address addr, String sessionID, String queueReceiverID) {

        if (addr == null) {
            throw new NullPointerException("null address");
        }
        if (sessionID == null) {
            throw new NullPointerException("null session ID");
        }
        if (queueReceiverID == null) {
            throw new NullPointerException("null queue receiver ID");
        }

        this.addr = addr;
        this.sessionID = sessionID;
        this.queueReceiverID = queueReceiverID;
    }

    public Address getAddress() {
        return addr;
    }

    public String getSessionID() {
        return sessionID;
    }

    public String getReceiverID() {
        return queueReceiverID;
    }

    public boolean isNextForDelivery() {
        return nextForDelivery;
    }

    public void setNextForDelivery(boolean b) {
        nextForDelivery = b;
    }

    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        
        if (!(o instanceof QueueReceiverAddress)) {
            return false;
        }

        QueueReceiverAddress that = (QueueReceiverAddress)o;

        return 
            (addr != null && addr.equals(that.addr)) && 
            (sessionID != null && sessionID.equals(that.sessionID)) &&
            (queueReceiverID != null && queueReceiverID.equals(that.queueReceiverID));
        
    }

    public int hashCode() {

        // TO_DO
        
        return 
            (addr == null ? 0 : addr.hashCode()) +
            (sessionID == null ? 0 : sessionID.hashCode()) +
            (queueReceiverID == null ? 0 : queueReceiverID.hashCode());
            
    }

    public String toString() {
        StringBuffer sb = new StringBuffer("QueueReceiverAddress[");
        sb.append(addr);
        sb.append("/sessionID=");
        sb.append(sessionID);
        sb.append("/receiverID=");
        sb.append(queueReceiverID);
        sb.append("]");
        return sb.toString();
    }

}

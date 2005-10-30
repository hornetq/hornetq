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
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import org.jgroups.util.Util;
import java.util.Iterator;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class GroupState {

    private static final Logger log = Logger.getLogger(GroupState.class);

    private Map queues;

    public synchronized byte[] toByteBuffer() throws Exception {
        return Util.objectToByteBuffer(queues);
    }

    public synchronized void fromByteBuffer(byte[] ba) throws Exception {
        Object o = Util.objectFromByteBuffer(ba);
        if (o == null) {
            queues = null;
        }
        else if (o instanceof Map) {
            queues = (Map)o;
        }
        else {
            throw new IllegalStateException("Invalid group state");
        }
    }


    public synchronized void addQueueReceiver(String queueName, Address addr, String sessionID,
                                              String queueReceiverID) {
        if (queues == null) {
            queues = new HashMap();
        }
        List l = (List)queues.get(queueName);
        if (l == null) {
            l = new ArrayList();
            queues.put(queueName, l);
        }
        QueueReceiverAddress ra = new QueueReceiverAddress(addr, sessionID, queueReceiverID);
        if (l.contains(ra)) {
            log.warn(ra+" already in the group state");
            return;
        }
        l.add(ra);
        log.debug("New GroupState: "+toString());
    }

    /**
     * If no such queue receiver is found, the method logs the event as a warning.
     **/
    public synchronized void removeQueueReceiver(String queueName, Address addr, String sessionID,
                                                 String queueReceiverID) {

        String noSuchReceiverMsg = 
            "No such queue receiver: "+queueName+"/"+addr+"/"+sessionID+"/"+queueReceiverID;

        List l = null;

        if (queues == null || 
            ((l = (List)queues.get(queueName)) == null) ||
            l.isEmpty()) {
            log.warn(noSuchReceiverMsg);
        }
        if (!l.remove(new QueueReceiverAddress(addr, sessionID, queueReceiverID))) {
            log.warn(noSuchReceiverMsg);
        }
        log.debug("New GroupState: "+toString());
    }


    /**
     * Could return null if there is no receiver for the queue
     **/
    public synchronized QueueReceiverAddress selectReceiver(String queueName) {
        
        if (queues == null) {
            return null;
        }
        List l = (List)queues.get(queueName);
        if (l == null || l.size() == 0) {
            return null;
        }
        QueueReceiverAddress selected = null;
        int crtidx = 0;
        for(Iterator i = l.iterator(); i.hasNext(); crtidx++) {
            QueueReceiverAddress crt = (QueueReceiverAddress)i.next();
            if (crt.isNextForDelivery()) {
                selected = crt;
                crt.setNextForDelivery(false);
                ((QueueReceiverAddress)l.get((crtidx + 1) % l.size())).setNextForDelivery(true);
                break;
            }
        }
        if (selected == null) {
            selected = (QueueReceiverAddress)l.get(0);
            ((QueueReceiverAddress)l.get(1 % l.size())).setNextForDelivery(true);;
            
        }
        return selected;
    }


    public String toString() {
        return queues == null ? "null" : queues.toString();
    }

}

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

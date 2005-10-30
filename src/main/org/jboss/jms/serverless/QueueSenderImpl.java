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

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
import javax.jms.MessageProducer;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class MessageProducerImpl implements MessageProducer {

    private static final Logger log = Logger.getLogger(MessageProducerImpl.class);

    private SessionImpl session;
    private Destination destination;

    MessageProducerImpl(SessionImpl session, Destination destination) {

        this.session = session;
        this.destination = destination;

        // TO_DO
    }


    //
    // MessageProducer INTERFACE IMPLEMENTATION
    // 


    public void setDisableMessageID(boolean value) throws JMSException {
        throw new NotImplementedException();
    }

    public boolean getDisableMessageID() throws JMSException {
        throw new NotImplementedException();
    }

    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        throw new NotImplementedException();
    }

    public boolean getDisableMessageTimestamp() throws JMSException {
        throw new NotImplementedException();
    }

    public void setDeliveryMode(int deliveryMode) throws JMSException {
        throw new NotImplementedException();
    }

    public int getDeliveryMode() throws JMSException {
        throw new NotImplementedException();
    }

    public void setPriority(int defaultPriority) throws JMSException {
        throw new NotImplementedException();
    }

    public int getPriority() throws JMSException {
        throw new NotImplementedException();
    }

    public void setTimeToLive(long timeToLive) throws JMSException {
        throw new NotImplementedException();
    }
 
    public long getTimeToLive() throws JMSException {
        throw new NotImplementedException();
    }

    public Destination getDestination() throws JMSException {
        return destination;
    }
    
    public void close() throws JMSException {
        throw new NotImplementedException();
    }
    
    public void send(Message message) throws JMSException {

        message.setJMSDestination(destination);
        session.send(message);

    }
    
    public void send(Message message, int deliveryMode, int priority, long timeToLive)
        throws JMSException {
        throw new NotImplementedException();
    }
    
    public void send(Destination destination, Message message) throws JMSException {
        throw new NotImplementedException();
    }
 
    public void send(Destination destination, 
                     Message message, 
                     int deliveryMode, 
                     int priority,
                     long timeToLive) throws JMSException {
        throw new NotImplementedException();
    }
    
}

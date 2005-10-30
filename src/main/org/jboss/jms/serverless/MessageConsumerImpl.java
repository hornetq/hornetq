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
import javax.jms.MessageListener;
import javax.jms.MessageConsumer;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
abstract class MessageConsumerImpl implements MessageConsumer {

    private static final Logger log = Logger.getLogger(MessageConsumerImpl.class);

    protected SessionImpl session;
    private MessageListener listener;
    private Destination destination;

    MessageConsumerImpl(SessionImpl session, Destination destination) {

        this.session = session;
        this.destination = destination;
    }

    Destination getDestination() {
        return destination;
    }

    //
    // MessageConsumer INTERFACE IMPLEMENTATION
    //

    public String getMessageSelector() throws JMSException {
        throw new NotImplementedException();
    }

    public MessageListener getMessageListener() throws JMSException {
        return listener;
    }

    public void setMessageListener(MessageListener listener) throws JMSException {
        this.listener = listener;
    }

    public Message receive() throws JMSException {
        throw new NotImplementedException();
    }

    public Message receive(long timeout) throws JMSException {
        throw new NotImplementedException();
    }

    public Message receiveNoWait() throws JMSException {
        throw new NotImplementedException();
    }

    public abstract void close() throws JMSException;

}

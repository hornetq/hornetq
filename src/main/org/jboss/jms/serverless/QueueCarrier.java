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
import java.io.Serializable;

/**
 * JMS Message wrapper. It carries additional information that helps the JMS provider to
 * route the messages to its final destination.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class QueueCarrier implements Serializable {

    static final long serialVersionUID = 214803107211354L;

    private String sessionID;
    private String queueReceiverID;
    private javax.jms.Message jmsMessage;

    public QueueCarrier(javax.jms.Message jmsMessage) {
        this(null, null, jmsMessage);
    }


    public QueueCarrier(String sessionID, String queueReceiverID, javax.jms.Message jmsMessage) {
        this.sessionID = sessionID;
        this.queueReceiverID = queueReceiverID;
        this.jmsMessage = jmsMessage;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void setSessionID(String id) {
        sessionID = id;
    }

    public String getReceiverID() {
        return queueReceiverID;
    }

    public void setReceiverID(String id) {
        queueReceiverID = id;
    }

    public javax.jms.Message getJMSMessage() {
        return jmsMessage;
    }
}

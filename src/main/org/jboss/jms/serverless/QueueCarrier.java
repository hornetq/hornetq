/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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

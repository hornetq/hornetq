/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless.client;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.JMSException;

/**
 * A simple JMS client that receives messages from a queue. It uses the common JMS 1.1 interfaces.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 **/
public class CommonInterfaceQueueReceiver {

    private static final Logger log = Logger.getLogger(CommonInterfaceQueueReceiver.class);

    /**
     **/
    public static void main(String[] args) throws Exception {

        Context initialContext = new InitialContext();

        ConnectionFactory connectionFactory = 
            (ConnectionFactory)initialContext.lookup("ConnectionFactory");

        Destination queue = (Destination)initialContext.lookup("Queue1");

        Connection connection = connectionFactory.createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        session.createConsumer(queue).setMessageListener(new MessageListenerImpl("Receiver ONE"));
        session.createConsumer(queue).setMessageListener(new MessageListenerImpl("Receiver TWO"));

        connection.start();
        log.info("Connection started, waiting for messages ...");
    } 


    static class MessageListenerImpl implements MessageListener {

        private String receiverName;

        public MessageListenerImpl(String receiverName) {

            this.receiverName = receiverName;
        }

        public void onMessage(Message message) {
                    
            try {
                log.info(receiverName+" got message: "+((TextMessage)message).getText());
            }
            catch(JMSException e) {
                log.error("Error handling the message", e);
            }
        }
    }

}




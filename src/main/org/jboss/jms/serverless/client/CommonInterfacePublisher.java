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
import javax.jms.Destination;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * A simple JMS client that publishes messages on a topic. It uses the common JMS 1.1 interfaces.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 **/
public class CommonInterfacePublisher {

    private static final Logger log = Logger.getLogger(CommonInterfacePublisher.class);

    private static final int DEFAULT_NUMBER_OF_MESSAGES = 10;

    /**
     **/
    public static void main(String[] args) throws Exception {

        Context initialContext = new InitialContext();

        ConnectionFactory connectionFactory = 
            (ConnectionFactory)initialContext.lookup("ConnectionFactory");

        Destination topic = (Destination)initialContext.lookup("Topic1");

        Connection connection = connectionFactory.createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        MessageProducer producer = session.createProducer(topic);
        connection.start();
        Thread.sleep(1000);

        int numberOfMessages = getNumberOfMessages(args);
        log.info("Sending "+numberOfMessages+" text messages ...");

        for(int i = 0; i < numberOfMessages; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("This is message "+i); 
            producer.send(message);
            log.debug("sent message "+i);
        }

        TextMessage message = session.createTextMessage();
        message.setText("");
        producer.send(message);
        log.debug("sent end-of-communication message");

        log.info("Finished sending messages");

        // TO_DO: If I immediately close the producer after sending the messages, sometimes the 
        // view change arrives before the messages, which are then discared by NACKACK. 
//         Thread.sleep(1000);
//         connection.close();
//         log.info("Successfully closed the connection");
//         System.exit(0);
    } 


    private static int getNumberOfMessages(String[] args) {
        
        int result = DEFAULT_NUMBER_OF_MESSAGES;

        if (args.length > 0) {
            try {
                result = Integer.parseInt(args[0]);
            }
            catch(Exception e) {
                log.warn("Invalid number of messages: "+args[0]);
            }
        }

        return result;
    }

}




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
 * A simple JMS client that consumes messages from a topic. It uses the common JMS 1.1 interfaces.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 **/
public class CommonInterfaceSubscriber {

    private static final Logger log = Logger.getLogger(CommonInterfaceSubscriber.class);

    private static long counter = 0;
    private static long startTimestamp = 0;
    private static long stopTimestamp = 0;

    /**
     **/
    public static void main(String[] args) throws Exception {

        Context initialContext = new InitialContext();

        ConnectionFactory connectionFactory = 
            (ConnectionFactory)initialContext.lookup("ConnectionFactory");

        Destination topic = (Destination)initialContext.lookup("Topic1");

        Connection connection = connectionFactory.createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        MessageConsumer consumer = session.createConsumer(topic);
        consumer.setMessageListener(new MessageListener() {
                
                public void onMessage(Message message) {

                    if (startTimestamp == 0) {
                        startTimestamp = System.currentTimeMillis();
                    }
                    
                    try {
                        TextMessage tm = (TextMessage)message;
                        String text = tm.getText();
                        log.debug("Got message: "+text);

                        if (!"".equals(text)) {
                            counter++;
                            if(counter % 1000 == 0) {
                                System.out.println(counter);
                            }
                        }
                        else {
                            stopTimestamp = System.currentTimeMillis();
                            long elapsed = stopTimestamp - startTimestamp;
                            int msgPerSec = (int)(((float)counter) / elapsed * 1000);
                            log.info("Received "+counter+" messages in " +
                                     elapsed + " ms, "+msgPerSec+" messages per second");
                            System.exit(0);
                        }
                    }
                    catch(JMSException e) {
                        log.error("Error handling the message", e);
                    }
                }
            }); 


        connection.start();
        log.info("Connection started, waiting for messages ...");
    } 

}




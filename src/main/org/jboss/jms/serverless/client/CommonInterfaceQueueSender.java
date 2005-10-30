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
 * A simple JMS client that sends messages to a queue. It uses the common JMS 1.1 interfaces.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 **/
public class CommonInterfaceQueueSender {

    private static final Logger log = Logger.getLogger(CommonInterfaceQueueSender.class);

    private static final int DEFAULT_NUMBER_OF_MESSAGES = 10;

    /**
     **/
    public static void main(String[] args) throws Exception {

        Context initialContext = new InitialContext();

        ConnectionFactory connectionFactory = 
            (ConnectionFactory)initialContext.lookup("ConnectionFactory");

        Destination queue = (Destination)initialContext.lookup("Queue1");

        Connection connection = connectionFactory.createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        MessageProducer sender = session.createProducer(queue);

        int numberOfMessages = getNumberOfMessages(args);
        log.info("Sending "+numberOfMessages+" text messages ...");

        for(int i = 0; i < numberOfMessages; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("QUEUE MESSAGE ["+i+"]");
            sender.send(message);
            log.info("Sent message "+i);
        }

        log.info("Finished sending messages");

        // TO_DO: If I immediately close the producer after sending the messages, sometimes the 
        // view change arrives before the messages, which are then discared by NACKACK. 
//         Thread.sleep(3000);
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




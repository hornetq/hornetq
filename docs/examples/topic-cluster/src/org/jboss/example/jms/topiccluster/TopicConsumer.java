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
package org.jboss.example.jms.topiccluster;

import javax.naming.InitialContext;
import javax.jms.*;


/**
 * @author Clebert Suconic
 */
public class TopicConsumer extends Thread
{

    InitialContext ic;
    String destinationName;
    String serverName;
    public int numberOfMessagesReceived=0;
    public TopicConsumer(InitialContext ctx, String destinationName, String serverName)
    {
        this.ic=ctx;
        this.destinationName=destinationName;
        this.serverName=serverName;
    }
    public void run()
    {
        try
        {
            ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

            Topic topic = (Topic)ic.lookup(destinationName);

            Connection connection = cf.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer subscriber = session.createConsumer(topic);

            ExampleListener messageListener = new ExampleListener(serverName);
            subscriber.setMessageListener(messageListener);
            connection.start();


            messageListener.waitForMessage(1);

             TextMessage messages[] = (TextMessage[])messageListener.getMessages();
             for (int i=0;i<messages.length;i++)
             {
                 TextMessage message = messages[i];
                 System.out.println("Message received on " + serverName + " result=" + message.getText());
                 numberOfMessagesReceived++;
             }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}

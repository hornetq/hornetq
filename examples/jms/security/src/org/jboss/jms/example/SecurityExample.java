/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * A simple JMS example that shows how to access messaging with security configured.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class SecurityExample extends JMSExample
{
   private boolean result = true;
   
   public static void main(String[] args)
   {
      new SecurityExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection billConnection = null;
      Connection andrewConnection = null;
      Connection frankConnection = null;
      Connection samConnection = null;
      
      InitialContext initialContext = null;
      try
      {
         ///Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. perform lookup on the topics
         Topic genericTopic = (Topic) initialContext.lookup("/topic/genericTopic");
         Topic europeTopic = (Topic) initialContext.lookup("/topic/europeTopic");
         Topic usTopic = (Topic) initialContext.lookup("/topic/usTopic");

         //Step 3. perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4. Try to create a JMS Connection without user/password. It will fail.
         try
         {
            Connection connection = cf.createConnection();
            result = false;
         }
         catch (JMSSecurityException e)
         {
            System.out.println("Default user cannot get a connection. Details: " + e.getMessage());
         }

         //Step 5. bill tries to make a connection using wrong password
         billConnection = null;
         try
         {
            billConnection = createConnection("bill", "jbossmessaging1", cf);
            result = false;
         }
         catch (JMSException e)
         {
            System.out.println("Bill failed to connect. Details: " + e.getMessage());
         }
         
         //Step 6. bill makes a good connection.
         billConnection = createConnection("bill", "jbossmessaging", cf);
         billConnection.start();
         
         //Step 7. andrew makes a good connection.
         andrewConnection = createConnection("andrew", "jbossmessaging1", cf);
         andrewConnection.start();
         
         //Step 8. frank makes a good connection.
         frankConnection = createConnection("frank", "jbossmessaging2", cf);
         frankConnection.start();
         
         //Step 9. sam makes a good connection.
         samConnection = createConnection("sam", "jbossmessaging3", cf);
         samConnection.start();
         
         //Step 10. Check every user can publish/subscribe genericTopics.
         System.out.println("------------------------Checking permissions on " + genericTopic + "----------------");
         checkUserSendAndReceive(genericTopic, billConnection, "bill");
         checkUserSendAndReceive(genericTopic, andrewConnection, "andrew");
         checkUserSendAndReceive(genericTopic, frankConnection, "frank");
         checkUserSendAndReceive(genericTopic, samConnection, "sam");
         System.out.println("-------------------------------------------------------------------------------------");
         
         //Step 11. Check permissions on europeTopic
         System.out.println("------------------------Checking permissions on " + europeTopic + "----------------");
         checkUserNoSendNoReceive(europeTopic, billConnection, "bill", andrewConnection, frankConnection);
         checkUserSendNoReceive(europeTopic, andrewConnection, "andrew", frankConnection);
         checkUserReceiveNoSend(europeTopic, frankConnection, "frank", andrewConnection);
         checkUserReceiveNoSend(europeTopic, samConnection, "sam", andrewConnection);
         System.out.println("-------------------------------------------------------------------------------------");
         
         //Step 12. Check permissions on usTopic
         System.out.println("------------------------Checking permissions on " + usTopic + "----------------");
         checkUserNoSendNoReceive(usTopic, billConnection, "bill", frankConnection, frankConnection);
         checkUserNoSendNoReceive(usTopic, andrewConnection, "andrew", frankConnection, frankConnection);
         checkUserSendAndReceive(usTopic, frankConnection, "frank");
         checkUserReceiveNoSend(usTopic, samConnection, "sam", frankConnection);
         System.out.println("-------------------------------------------------------------------------------------");

         return result;
      }
      finally
      {
         //Step 16. Be sure to close our JMS resources!
         if (billConnection != null)
         {
            billConnection.close();
         }
         if (andrewConnection != null)
         {
            andrewConnection.close();
         }
         if (frankConnection != null)
         {
            frankConnection.close();
         }
         if (samConnection != null)
         {
            samConnection.close();
         }
         
         // Also the initialContext
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }


   //Check the user can receive message but cannot send message.
   private void checkUserReceiveNoSend(Topic topic, Connection connection, String user, Connection sendingConn) throws JMSException
   {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(topic);
      MessageConsumer consumer = session.createConsumer(topic);
      TextMessage msg = session.createTextMessage("hello-world-1");
      producer.send(msg);
      TextMessage receivedMsg = (TextMessage)consumer.receive(2000);
      if (receivedMsg == null)
      {
         System.out.println("User " + user + " cannot send message [" + msg.getText() + "] to topic " + topic);
      }
      else
      {
         System.out.println("Security setting is broken! User " + user + " can send message [" + receivedMsg.getText() + "] to topic " + topic);
         result = false;
      }

      //Now send a good message
      Session session1 = sendingConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      producer = session1.createProducer(topic);
      producer.send(msg);
      
      receivedMsg = (TextMessage)consumer.receive(2000);
      
      if (receivedMsg != null)
      {
         System.out.println("User " + user + " can receive message [" + receivedMsg.getText() + "] from topic " + topic);
      }
      else
      {
         System.out.println("Security setting is broken! User " + user + " cannot receive message from topic " + topic);
         result = false;         
      }
      session.close();
   }

   //Check the user can send message but cannot receive message
   private void checkUserSendNoReceive(Topic topic, Connection connection, String user, Connection receivingConn) throws JMSException
   {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(topic);
      MessageConsumer consumer = null;
      try
      {
         consumer = session.createConsumer(topic);
      }
      catch (JMSException e)
      {
         System.out.println("User " + user + " cannot receive any message from topic " + topic);
      }

      Session session1 = receivingConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer goodConsumer = session1.createConsumer(topic);
      
      TextMessage msg = session.createTextMessage("hello-world-2");
      producer.send(msg);
      
      TextMessage receivedMsg = (TextMessage)goodConsumer.receive(2000);
      if (receivedMsg != null)
      {
         System.out.println("User " + user + " can send message [" + receivedMsg.getText() + "] to topic " + topic);
      }
      else
      {
         System.out.println("Security setting is broken! User " + user + " cannot send message [" + msg.getText() + "] to topic " + topic);
         result = false;
      }
      
      if (consumer != null)
      {
         receivedMsg = (TextMessage)consumer.receive(2000);
         if (receivedMsg == null)
         {
            System.out.println("User " + user + " cannot receive any message from topic " + topic);
         }
         else
         {
            System.out.println("Security setting is broken! User " + user + " can receive message [" + receivedMsg.getText() + "] from topic " + topic);
            result = false;
         }
      }
      
      session.close();
      session1.close();
   }

   //Check the user has neither send nor receive permission on topic
   private void checkUserNoSendNoReceive(Topic topic, Connection connection, String user, Connection sendingConn, Connection receivingConn) throws JMSException
   {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(topic);
      MessageConsumer consumer = null;
      
      try
      {
         consumer = session.createConsumer(topic);
      }
      catch (JMSException e)
      {
         System.out.println("User " + user + " cannot create consumer on topic " + topic);
      }
      
      Session session1 = receivingConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer goodConsumer = session1.createConsumer(topic);
      
      TextMessage msg = session.createTextMessage("hello-world-3");
      producer.send(msg);

      TextMessage receivedMsg = (TextMessage)goodConsumer.receive(2000);
      
      if (receivedMsg == null)
      {
         System.out.println("User " + user + " cannot send message [" + msg.getText() + "] to topic: " + topic);
      }
      else
      {
         System.out.println("Security setting is broken! User " + user + " can send message [" + msg.getText() + "] to topic " + topic);
         result = false;
      }
      
      if (consumer != null)
      {
         Session session2 = sendingConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer goodProducer = session2.createProducer(topic);
         goodProducer.send(msg);
      
         receivedMsg = (TextMessage)consumer.receive(2000);
      
         if (receivedMsg == null)
         {
            System.out.println("User " + user + " cannot receive message [" + msg.getText() + "] from topic " + topic);
         }
         else
         {
            System.out.println("Security setting is broken! User " + user + " can receive message [" + receivedMsg.getText() + "] from topic " + topic);
         }
         session2.close();
      }
      
      session.close();
      session1.close();
   }

   //Check the user connection has both send and receive permissions on the topic
   private void checkUserSendAndReceive(Topic topic, Connection connection, String user) throws JMSException
   {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TextMessage msg = session.createTextMessage("hello-world-4");
      MessageProducer producer = session.createProducer(topic);
      MessageConsumer consumer = session.createConsumer(topic);
      producer.send(msg);
      TextMessage receivedMsg = (TextMessage)consumer.receive(5000);
      if (receivedMsg != null)
      {
         System.out.println("User " + user + " can send message: [" + msg.getText() + "] to topic: " + topic);
         System.out.println("User " + user + " can receive message: [" + msg.getText() + "] from topic: " + topic);
      }
      else
      {
         System.out.println("Error! User " + user + " cannot receive the message! ");
         result = false;
      }
      session.close();
   }

   private Connection createConnection(String username, String password, ConnectionFactory cf) throws JMSException
   {
      return cf.createConnection(username, password);
   }
}

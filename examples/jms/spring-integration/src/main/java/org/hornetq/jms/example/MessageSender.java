package org.hornetq.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class MessageSender
{
   private ConnectionFactory connectionFactory;
   private Destination destination;

   public ConnectionFactory getConnectionFactory()
   {
      return connectionFactory;
   }

   public void setConnectionFactory(ConnectionFactory connectionFactory)
   {
      this.connectionFactory = connectionFactory;
   }

   public Destination getDestination()
   {
      return destination;
   }

   public void setDestination(Destination destination)
   {
      this.destination = destination;
   }

   public void send(String msg)
   {
      try
      {
         Connection conn = connectionFactory.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(destination);
         TextMessage message = session.createTextMessage(msg);
         producer.send(message);
      }
      catch (Exception ex)
      {
         ex.printStackTrace();
      }
   }
}

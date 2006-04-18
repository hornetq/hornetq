/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.ejb3mdb;

import javax.ejb.MessageDriven;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.PostConstruct;
import javax.naming.InitialContext;
import javax.jms.TextMessage;
import javax.jms.Session;
import javax.jms.MessageListener;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Connection;
import javax.jms.MessageProducer;

/**
 * A MDB3 EJB example.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>

 * $Id$
 */
@MessageDriven(activateConfig =
{
      @ActivationConfigProperty(propertyName="destinationType", propertyValue="javax.jms.Queue"),
      @ActivationConfigProperty(propertyName="destination", propertyValue="queue/testQueue")
})
public class EJB3MDBExample implements MessageListener
{
   public void onMessage(Message m)
   {
      businessLogic(m);
   }


   private void businessLogic(Message m)
   {
      Connection conn = null;
      Session session = null;

      try
      {
         TextMessage tm = (TextMessage)m;

         String text = tm.getText();
         System.out.println("message " + text + " received");

         // flip the string
         String result = "";
         for(int i = 0; i < text.length(); i++)
         {
            result = text.charAt(i) + result;
         }

         System.out.println("message processed, result: " + result);


         InitialContext ic = new InitialContext();
         ConnectionFactory cf = (ConnectionFactory)ic.lookup("java:/JmsXA");
         ic.close();

         conn = cf.createConnection();
         conn.start();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Destination replyTo = m.getJMSReplyTo();
         MessageProducer producer = session.createProducer(replyTo);
         TextMessage reply = session.createTextMessage(result);

         producer.send(reply);
         producer.close();

      }
      catch(Exception e)
      {
         e.printStackTrace();
         System.out.println("The Message Driven Bean failed!");
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch(Exception e)
            {
               System.out.println("Could not close the connection!" +e);
            }
         }
      }
   }
}




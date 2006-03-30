/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.mdb;

import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.Destination;
import javax.jms.Session;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.ejb.MessageDrivenBean;
import javax.ejb.MessageDrivenContext;
import javax.ejb.EJBException;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>

 * $Id$
 */
public class MDBExample implements MessageDrivenBean, MessageListener
{

   private MessageDrivenContext ctx;
   private Connection connection;
   private Session session;

   public void ejbCreate()
   {
      try
      {
         InitialContext ic = new InitialContext();

         ConnectionFactory cf = (ConnectionFactory)ic.lookup("java:/JmsXA");

         connection = cf.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         ic.close();
      }
      catch(Exception e)
      {
         e.printStackTrace();
         throw new EJBException("Initalization failure: " + e.getMessage());
      }
   }

   public void ejbRemove() throws EJBException
   {
      try
      {
         connection.close();
      }
      catch(Exception e)
      {
         throw new EJBException("Tear down failure", e);
      }
   }

   public void setMessageDrivenContext(MessageDrivenContext ctx)
   {
      this.ctx = ctx;
   }

   public void onMessage(Message m)
   {
      try
      {
         TextMessage tm = (TextMessage)m;
         String text = tm.getText();



         System.out.println("message " + text + " received");



         String result = process(text);



         System.out.println("message processed, result: " + result);



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
   }


   private String process(String text)
   {
      // flip the string

      String result = "";

      for(int i = 0; i < text.length(); i++)
      {
         result = text.charAt(i) + result;
      }
      return result;
   }

}

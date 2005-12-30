/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.stateless.bean;

import javax.jms.TextMessage;
import javax.jms.Session;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;
import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;
import javax.naming.InitialContext;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Enumeration;
import java.util.ArrayList;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>

 * $Id$
 */
public class StatelessSessionExampleBean implements SessionBean
{

   private SessionContext ctx;
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
         connection.start();

         ic.close();
      }
      catch(Exception e)
      {
         e.printStackTrace();
         throw new EJBException("Initalization failure: " + e.getMessage());
      }
   }

   public void setSessionContext(SessionContext ctx) throws EJBException, RemoteException
   {
      this.ctx = ctx;
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

   public void ejbActivate() throws EJBException, RemoteException
   {
   }

   public void ejbPassivate() throws EJBException, RemoteException
   {
   }




   public void send(String txt, String queueName) throws Exception
   {
      InitialContext ic = new InitialContext();
      Queue queue = (Queue)ic.lookup(queueName);



      MessageProducer producer = session.createProducer(queue);



      TextMessage tm = session.createTextMessage(txt);



      producer.send(tm);
      System.out.println("message " + txt + " sent to " + queueName);



      producer.close();
      ic.close();
   }



   public List browse(String queueName) throws Exception
   {
      InitialContext ic = new InitialContext();
      Queue queue = (Queue)ic.lookup(queueName);



      QueueBrowser browser = session.createBrowser(queue);


      ArrayList list = new ArrayList();
      for(Enumeration e = browser.getEnumeration(); e.hasMoreElements(); )
      {
         list.add(e.nextElement());
      }


      browser.close();
      ic.close();


      return list;
   }



   public String receive(String queueName) throws Exception
   {
      InitialContext ic = new InitialContext();
      Queue queue = (Queue)ic.lookup(queueName);



      MessageConsumer consumer = session.createConsumer(queue);


      System.out.println("blocking to receive message from queue " + queueName + " ...");
      TextMessage tm = (TextMessage)consumer.receive(5000);

      if (tm == null)
      {
         throw new Exception("No message!");
      }


      System.out.println("Message " + tm.getText() + " received");

      consumer.close();
      ic.close();



      return tm.getText();
   }
}

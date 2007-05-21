/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.stateless.bean;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Enumeration;

import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 
 * $Id$
 */
public class StatelessSessionExampleBean implements SessionBean
{   

   private ConnectionFactory cf = null;
   
   public void drain(String queueName) throws Exception
   {
      InitialContext ic = new InitialContext();
      Queue queue = (Queue)ic.lookup(queueName);
      ic.close();
      
      Session session = null;
      Connection conn = null;
      
      try
      {
         conn = getConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);
         Message m = null;
         do
         {
            m = consumer.receive(1L);
         }
         while(m != null);
      }
      finally
      {
         if (conn != null)
         {
            closeConnection(conn);
         }
      }
   }
   
   public void send(String txt, String queueName) throws Exception
   {
      InitialContext ic = new InitialContext();
      
      Queue queue = (Queue)ic.lookup(queueName);
      
      ic.close();
      
      Session session = null;
      Connection conn = null;
      
      try
      {
         conn = getConnection();
         
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer producer = session.createProducer(queue);
         
         TextMessage tm = session.createTextMessage(txt);
         
         producer.send(tm);
         
         System.out.println("message " + txt + " sent to " + queueName);         
      }
      finally
      {
         if (conn != null)
         {
            closeConnection(conn);
         }
      }
   }
   
   public int browse(String queueName) throws Exception
   {
      InitialContext ic = new InitialContext();
      Queue queue = (Queue)ic.lookup(queueName);
      ic.close();
      
      Session session = null;
      Connection conn = null;
      
      try
      {
         conn = getConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         QueueBrowser browser = session.createBrowser(queue);
         
         ArrayList list = new ArrayList();
         for(Enumeration e = browser.getEnumeration(); e.hasMoreElements(); )
         {
            list.add(e.nextElement());
         }
         
         return list.size();
      }
      finally
      {
         if (conn != null)
         {
            closeConnection(conn);
         }
      }
   }
   
   public String receive(String queueName) throws Exception
   {
      InitialContext ic = new InitialContext();
      Queue queue = (Queue)ic.lookup(queueName);
      ic.close();
      
      Session session = null;
      Connection conn = null;
      
      try
      {
         conn = getConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer consumer = session.createConsumer(queue);
         
         System.out.println("blocking to receive message from queue " + queueName + " ...");
         TextMessage tm = (TextMessage)consumer.receive(5000);
         
         if (tm == null)
         {
            throw new Exception("No message!");
         }
         
         System.out.println("Message " + tm.getText() + " received");
         
         return tm.getText();         
      }
      finally
      {
         if (conn != null)
         {
            closeConnection(conn);
         }
      }
   }
   
   public Connection getConnection() throws Exception
   {
      
      Connection connection = null;
      
      try
      {
         connection = cf.createConnection();
         
         connection.start();         
      }
      catch(Exception e )
      {
         if(connection != null)
         {
            closeConnection(connection);
         }
         System.out.println("Failed to get connection...exception is " + e);
         throw e;
      }
      
      return connection;
   }
   
   public void closeConnection(Connection con) throws Exception
   {      
      try
      {
         con.close();         
      }
      catch(JMSException jmse)
      {
         System.out.println("Could not close connection " + con +" exception was " + jmse);
         throw jmse;
      }
   }
   
   public void setSessionContext(SessionContext ctx) throws EJBException, RemoteException
   {      
   }
   
   public void ejbCreate()
   {
      try
      {
         InitialContext ic = new InitialContext();
         
         cf = (ConnectionFactory)ic.lookup("java:/JmsXA");
         
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
         if(cf != null)
         {
            cf = null;
         }
      }
      catch(Exception e)
      {
         throw new EJBException("ejbRemove ", e);
      }
   }
   
   public void ejbActivate() throws EJBException, RemoteException
   {
   }
   
   public void ejbPassivate() throws EJBException, RemoteException
   {
   }
      
}

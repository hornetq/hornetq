/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.mdb;

import javax.ejb.EJBException;
import javax.ejb.MessageDrivenBean;
import javax.ejb.MessageDrivenContext;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 
 * $Id$
 */
public class MDBExample implements MessageDrivenBean, MessageListener
{
   private MessageDrivenContext ctx;
   
   private ConnectionFactory cf = null;
   
   public void onMessage(Message m)
   {
      Session session = null;
      Connection conn = null;
      
      try
      {
         TextMessage tm = (TextMessage)m;
         
         String text = tm.getText();
         System.out.println("message " + text + " received");
         String result = process(text);
         System.out.println("message processed, result: " + result);
         
         conn = getConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Destination replyTo = m.getJMSReplyTo();
         MessageProducer producer = session.createProducer(replyTo);
         TextMessage reply = session.createTextMessage(result);
         
         producer.send(reply);
         producer.close();
         
      }
      catch(Exception e)
      {
         ctx.setRollbackOnly();
         e.printStackTrace();
         System.out.println("The Message Driven Bean failed!");
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               closeConnection(conn);
            }
            catch(Exception e)
            {
               System.out.println("Could not close the connection!" +e);
            }
         }
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
      catch(JMSException e)
      {
         System.out.println("Could not close connection " + con + " exception was " + e);
      }
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
         throw new EJBException("Failure to get connection factory: " + e.getMessage());
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
         throw new EJBException("ejbRemove", e);
      }
   }
   
   public void setMessageDrivenContext(MessageDrivenContext ctx)
   {
      this.ctx = ctx;
   }
   
   
}

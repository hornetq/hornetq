package org.jboss.test.messaging.jms.managed;

import java.rmi.RemoteException;

import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;

public class JMSTestBean implements SessionBean
{
   
   private static Logger log = Logger.getLogger(JMSTestBean.class);
   
   private Connection connection;
   
   private Queue queue;
   
   
   private SessionContext ctx;
   
   public void setSessionContext(SessionContext sc)
   {
      ctx = sc;
   }
   
   public void ejbCreate()
   {
      Context context = null;
      ConnectionFactory connectionFactory = null;
      
      try
      {
         InitialContext iniCtx = new InitialContext();
 
         ConnectionFactory cf =
            (ConnectionFactory) iniCtx.lookup("java:comp/env/jms/MyConnectionFactory");         
         connection = cf.createConnection();
         connection.start();
         queue = (Queue)iniCtx.lookup("java:comp/env/jms/MyQueue");
      }
      catch (Throwable t)
      {           
         log.error(t);
      }
   }
   
   
   public void test1() throws EJBException
   {
      Session session = null;
      
      try
      {
         log.trace("In test1");
        
         
         session = 
            connection.createSession(true, 0);
         
         log.trace("Created session");
         
         MessageProducer producer = session.createProducer(queue);
         
         TextMessage message = session.createTextMessage();
         
         message.setText("Testing 123");
         
         producer.send(message);
         
         log.trace("Sent message");
         
      }
      catch (JMSException e)
      {           
         log.error(e);
         ctx.setRollbackOnly();
         throw new EJBException(e.toString());
      }
      finally
      {
         try
         {
            if (session != null) session.close();
         }
         catch (JMSException e)
         {            
         }         
      }
   }
   
   public void ejbRemove() throws RemoteException
   {
      
      try
      {
         if (connection != null) connection.close();
      } 
      catch (Exception e)
      {
         e.printStackTrace();
      }
      
   }
   
   public void ejbActivate() {}
   
   public void ejbPassivate() {}
}

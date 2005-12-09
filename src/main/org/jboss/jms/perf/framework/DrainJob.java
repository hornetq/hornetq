/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class DrainJob extends BaseJob
{
   private static final long serialVersionUID = -4637670167659745878L;

   private static final Logger log = Logger.getLogger(DrainJob.class);
   
   protected static final long RECEIVE_TIMEOUT = 5000;

   protected String subName;
   
   protected String clientID;
   
   protected Throwable throwable;
   
   
   public DrainJob(String slaveURL, Properties jndiProperties, String destinationName, String connectionFactoryJndiName,
         String subName, String clientID)
   {
      super(slaveURL, jndiProperties, destinationName, connectionFactoryJndiName);
      this.subName = subName;
      this.clientID = clientID;
   }
   

   public JobResult getResult()
   {
      JobResult res = new JobResult();
      res.failed = failed;
      if (failed)
      {
         res.throwables = new Throwable[] { throwable };
      }
      return res;
   }
   
   public Throwable[] getThrowables()
   {
      return new Throwable[] { throwable };
   }
   
   public void run()
   { 
      Connection conn = null;
      
      int count = 0;
      
      try
      {
 
         super.setup();
         
         log.info("Running drain job ===============");
         
         conn = cf.createConnection();
         
         if (clientID != null)
         {
            try
            {
               conn.setClientID(clientID);
            }
            catch (Exception e)
            {
               //Some providers may provide a connection with client id already set
            }
         }
         
         conn.start();
      
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
             
         MessageConsumer consumer;
         if (subName == null)
         {
         
            consumer = sess.createConsumer(dest);
         }
         else
         {
            consumer = sess.createDurableSubscriber((Topic)dest, subName);
         }
                    
         while (true)
         {
            Message m = consumer.receive(RECEIVE_TIMEOUT);
            //log.info("received message");
            if (m == null)
            {
               break;
            }
            count++;
         }
         
         log.info("Finished running job===================");         
      }
      catch (Throwable e)
      {
         log.error("Failed to drain destination", e);
         throwable = e;
         failed = true;
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Exception e)
            {
               log.error("Failed to close connection", e);
               throwable = e;
               failed = true;
            }
         }
      }
      

   } 
   
      
   public void setSubName(String subName)
   {
      this.subName = subName;
   }
   
   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

}

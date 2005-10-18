/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

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
   
   public DrainJob(String serverURL, String destinationName, String subName)
   {
      super(serverURL, destinationName);
      this.subName = subName;
   }
   
   public Object getResult()
   {
      return null;
   }
   
   public void run()
   { 
      Connection conn = null;
      
      int count = 0;
      
      try
      {
 
         super.setup();
         
         conn = cf.createConnection();
         
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
      catch (Exception e)
      {
         log.error("Failed to drain destination", e);
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
               failed = true;
            }
         }
      }
      

   } 
   
      
   public void setSubName(String subName)
   {
      this.subName = subName;
   }

}

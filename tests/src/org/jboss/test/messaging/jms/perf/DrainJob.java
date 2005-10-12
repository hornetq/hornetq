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

import org.jboss.logging.Logger;
import org.w3c.dom.Element;

public class DrainJob extends BaseJob
{
   private static final Logger log = Logger.getLogger(DrainJob.class);
   
   protected static final long RECEIVE_TIMEOUT = 5000;

   public String getName()
   {
      return "Drain Job";
   }
   
   public DrainJob()
   {
      
   }
   
   public DrainJob(Element e) throws ConfigurationException
   {
      super(e);
   }

   public JobResult run()
   {
      
      
      Connection conn = null;
      
      try
      {
         super.setup();
         
         conn = cf.createConnection();
      
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer consumer = sess.createConsumer(dest);
         
         while (true)
         {
            Message m = consumer.receive(RECEIVE_TIMEOUT);
            if (m == null)
            {
               break;
            }
         }
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
      
      return null;
   } 
   
   
   public void fillInResults(ResultPersistor persistor)
   {
      
   }

}

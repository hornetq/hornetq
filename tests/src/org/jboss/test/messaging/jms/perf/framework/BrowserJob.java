/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf.framework;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class BrowserJob extends BaseThroughputJob
{
   /** The serialVersionUID */
   private static final long serialVersionUID = -6915148242521181606L;

   private static final Logger log = Logger.getLogger(BrowserJob.class);

   protected String selector;
   
   public Servitor createServitor(int numMessages)
   {
      return new Browser(numMessages);
   }
   

   public BrowserJob(String slaveURL, Properties jndiProperties, String destinationName,
         String connectionFactoryJndiName, int numConnections,
         int numSessions, int numMessages, String selector)
   {
      super (slaveURL, jndiProperties, destinationName, connectionFactoryJndiName, numConnections,
            numSessions, false, 0,
            numMessages);
      this.selector = selector;
   }

   protected void logInfo()
   {
      super.logInfo();
      log.trace("Message selector: " + selector);
   }
   
   protected class Browser extends AbstractServitor
   {
      
      Browser(int numMessages)
      {
         super(numMessages);
      }
      
      Session sess;

      public void deInit()
      {
         try
         {
            sess.close();
         }      
         catch (Exception e)
         {
            log.error("!!!!!!!!!!!!!!!!!!Close failed", e);
            failed = true;
         }
      }
      
      public void init()
      {
         try
         {
            Connection conn = getNextConnection();
            
            sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            
         }
         catch (Exception e)
         {
            log.error("Receiver failed", e);
            failed = true;
         }
     
      }
      
      public void run()
      {
         try
         {
            QueueBrowser browser = sess.createBrowser((Queue)dest, selector);
                        
            int count = 0;
            
            Iterator iter = Collections.list(browser.getEnumeration()).iterator();
            
            while (count < numMessages && iter.hasNext())
            {
               Message m = (Message)iter.next();
               count++;               
            }
            
            if (count < numMessages)
            {
               failed = true;
               log.error("Not enough messages available to browse");
            }
         }
         catch (Exception e)
         {
            log.error("!!!!!!!!!!!!!!!Browser failed", e);
            failed = true;
         }
      }
      
      public boolean isFailed()
      {
         return failed;
      }
   }
   
   
   /**
    * Set the selector.
    * 
    * @param selector The selector to set.
    */
   public void setSelector(String selector)
   {
      this.selector = selector;
   }

   
}

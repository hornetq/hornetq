
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
 * 
 * A ReceiverJob.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ReceiverJob extends BaseThroughputJob
{
   private static final long serialVersionUID = 3633353742146810600L;
   
   private static final Logger log = Logger.getLogger(ReceiverJob.class);

   protected int ackMode;
   
   protected String subName;
   
   protected String selector;
   
   protected boolean noLocal;
   
   protected boolean asynch;
   
   protected String clientID;
   

   public Servitor createServitor(long testTime)
   {
      return new Receiver(testTime);
   }
   

   public ReceiverJob(Properties jndiProperties, String destinationName,
         String connectionFactoryJndiName, int numConnections,
         int numSessions, boolean transacted, int transactionSize,
         long testTime, int ackMode, String subName,
         String selector, boolean noLocal, boolean asynch, String clientID)
   {
      super (jndiProperties, destinationName, connectionFactoryJndiName, numConnections,
            numSessions, transacted, transactionSize,
            testTime);
      this.ackMode = ackMode;
      this.subName = subName;
      this.selector = selector;
      this.noLocal = noLocal;
      this.asynch = asynch;
      this.clientID = clientID;
   }

   protected class Receiver extends AbstractServitor
   {      
      Receiver(long testTime)
      {
         super(testTime);
      }
      
      Session sess;
      
      MessageConsumer cons;
      
      public void deInit()
      {
         log.info("de-Initialising");
         try
         {             
            if (subName != null)
            {
               sess.unsubscribe(subName);
            }
            
            sess.close();  
         }      
         catch (Throwable e)
         {
            log.error("deInit failed", e);
            failed = true;
         }
      }
      
      public void init()
      {
         log.info("initialising");
         try
         {
            Connection conn = getNextConnection();
            
            if (subName != null)
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
            
            sess = conn.createSession(transacted, ackMode);
            
            if (subName == null)
            {           
               cons = sess.createConsumer(dest, selector, noLocal);
            }
            else
            {
               cons = sess.createDurableSubscriber((Topic)dest, subName, selector, noLocal);
            }
         }
         catch (Throwable e)
         {
            log.error("Init failed", e);
            failed = true;
         }
     
      }
      
      public void run()
      {
         try
         {
            long start = System.currentTimeMillis();
            
            long now = 0;
            
            while (true)
            {                         
               now = System.currentTimeMillis();
               
               if (now > start + testTime)
               {
                  break;
               }
                             
               Message m;
               
               if (messageCount == 0)
               {
                  //Give it more time for the first message
                  m = cons.receive(60 * 1000);  
               }
               else
               {
                  m = cons.receive(10000);
               }
                      
               if (m != null)
               {
                  //log.info("Received");
                  
                  if (transacted)
                  {
                     if (messageCount % transactionSize == 0)
                     {
                        sess.commit();
                     }
                  }                
                  
                  messageCount++;
                  
               }    
               else
               {
                  log.info("No more messages");
                  
                  break;
               }                               
            }  
            
            actualTime = now - start;                           
         }
         catch (Throwable e)
         {
            log.error("Receiver failed", e);
            failed = true;
         }
      }      
   }
   
   
   /**
    * Set the ackMode.
    * 
    * @param ackMode The ackMode to set.
    */
   public void setAckMode(int ackMode)
   {
      this.ackMode = ackMode;
   }

   /**
    * Set the asynch.
    * 
    * @param asynch The asynch to set.
    */
   public void setAsynch(boolean asynch)
   {
      this.asynch = asynch;
   }

   /**
    * Set the noLocal.
    * 
    * @param noLocal The noLocal to set.
    */
   public void setNoLocal(boolean noLocal)
   {
      this.noLocal = noLocal;
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

   /**
    * Set the name.
    * 
    * @param subName The name to set.
    */
   public void setSubName(String subName)
   {
      this.subName = subName;
   }
   
   
   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }
}

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

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ReceiverJob extends BaseThroughputJob
{
   private static final long serialVersionUID = 3633353742146810600L;
   
   private static final long RECEIVE_TIMEOUT = 1000;

   private static final Logger log = Logger.getLogger(SenderJob.class);

   protected int ackMode;
   
   protected String subName;
   
   protected String selector;
   
   protected boolean noLocal;
   
   protected boolean asynch;
   

   public Servitor createServitor(int numMessages)
   {
      return new Receiver(numMessages);
   }
   

   public ReceiverJob(String serverURL, String destinationName, int numConnections,
         int numSessions, boolean transacted, int transactionSize,
         int numMessages, int ackMode, String subName,
         String selector, boolean noLocal, boolean asynch)
   {
      super (serverURL, destinationName, numConnections,
            numSessions, transacted, transactionSize,
            numMessages);
      this.ackMode = ackMode;
      this.subName = subName;
      this.selector = selector;
      this.noLocal = noLocal;
      this.asynch = asynch;
   }

   protected void logInfo()
   {
      super.logInfo();
      log.info("Acknowledgement Mode? " + ackMode);
      log.info("Durable subscription name: " + subName);
      log.info("Message selector: " + selector);
      log.info("No local?: " + noLocal);
      log.info("Use message listener? " + asynch);
   }
   
   protected class Receiver extends AbstractServitor
   {
      
      Receiver(int numMessages)
      {
         super(numMessages);
      }
      
      Session sess;
      
      MessageConsumer cons;
      
      public void deInit()
      {
         try
         {
            sess.close();
         }      
         catch (Exception e)
         {
            log.error("Receiver failed", e);
            failed = true;
         }
      }
      
      public void init()
      {
         try
         {
            Connection conn = getNextConnection();
            
            sess = conn.createSession(transacted, ackMode);
           
            cons = sess.createConsumer(dest, selector, noLocal);
                   
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
            log.info("Running receiver");
            
               
            int count = 0;
            
            while (count < (numMessages))
            {                           
               
               Message m = cons.receive(RECEIVE_TIMEOUT);  
                      
               if (m != null)
               {
                  count++;
                  if (transacted)
                  {
                     if (count % transactionSize == 0)
                     {
                        sess.commit();
                     }
                  } 
               }    
               else
               {
                  failed = true;
                  break;
               }
                                                         
            }  
         }
         catch (Exception e)
         {
            log.error("Receiver failed", e);
            failed = true;
         }
      }
      
      public boolean isFailed()
      {
         return failed;
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
    * Set the subName.
    * 
    * @param subName The subName to set.
    */
   public void setSubName(String subName)
   {
      this.subName = subName;
   }
   
}

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
public class ReceiverJob extends BaseThroughputJob
{
   private static final long serialVersionUID = 3633353742146810600L;
   
   private static final long RECEIVE_TIMEOUT = 10 * 60 * 1000;

   private static final Logger log = Logger.getLogger(SenderJob.class);

   protected int ackMode;
   
   protected String subName;
   
   protected String selector;
   
   protected boolean noLocal;
   
   protected boolean asynch;
   
   protected String clientID;
   

   public Servitor createServitor(int numMessages)
   {
      return new Receiver(numMessages);
   }
   

   public ReceiverJob(String slaveURL, Properties jndiProperties, String destinationName,
         String connectionFactoryJndiName, int numConnections,
         int numSessions, boolean transacted, int transactionSize,
         int numMessages, int ackMode, String subName,
         String selector, boolean noLocal, boolean asynch, String clientID)
   {
      super (slaveURL, jndiProperties, destinationName, connectionFactoryJndiName, numConnections,
            numSessions, transacted, transactionSize,
            numMessages);
      this.ackMode = ackMode;
      this.subName = subName;
      this.selector = selector;
      this.noLocal = noLocal;
      this.asynch = asynch;
      this.clientID = clientID;
   }

   protected void logInfo()
   {
      super.logInfo();
      log.trace("Acknowledgement Mode? " + ackMode);
      log.trace("Durable subscription name: " + subName);
      log.trace("Message selector: " + selector);
      log.trace("No local?: " + noLocal);
      log.trace("Use message listener? " + asynch);
      log.trace("Client id: " + clientID);
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
            throwable = e;
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
            throwable = e;
            failed = true;
         }
     
      }
      
      public void run()
      {
         try
         {
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
                  log.error("!!!!!!!!!!!!!!Failed to receive messages!!!!");
                  failed = true;
                  break;
               }
                                                         
            }  
         }
         catch (Throwable e)
         {
            log.error("Receiver failed", e);
            throwable = e;
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
    * Set the subName.
    * 
    * @param subName The subName to set.
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
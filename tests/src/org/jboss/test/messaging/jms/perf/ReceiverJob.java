
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

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ReceiverJob extends AbstractJob
{
   private static final long serialVersionUID = 3633353742146810600L;

   private static final Logger log = Logger.getLogger(SenderJob.class);

   protected int ackMode;
   
   protected String subName;
   
   protected String selector;
   
   protected boolean noLocal;
   
   protected boolean asynch;
   
   public void getResults(ResultPersistor persistor)
   {
      super.getResults(persistor);
      
      persistor.addValue("ackMode", ackMode);
      persistor.addValue("subName", subName);
      persistor.addValue("selector", selector);
      persistor.addValue("noLocal", noLocal);
      persistor.addValue("asynch", asynch);
   }
   
   
   public Servitor createServitor()
   {
      return new Receiver();
   }
   
   public String getName()
   {
      return "Receiver";
   }
   
   public ReceiverJob(Element e) throws DeploymentException
   {
      importXML(e);
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
   
   public void importXML(Element element) throws DeploymentException
   {  
      super.importXML(element);
      
      this.ackMode = Integer.parseInt(getUniqueChildContent(element, "acknowledgement-mode"));
      this.subName = getOptionalChildContent(element, "durable-subscription-name", null);
      this.selector = getOptionalChildContent(element, "mesage-selector", null);
      this.noLocal = getOptionalChildBooleanContent(element, "no-local", false);
      this.asynch = getOptionalChildBooleanContent(element, "use-listener", false);
      
      if (ackMode != Session.AUTO_ACKNOWLEDGE && ackMode != Session.CLIENT_ACKNOWLEDGE &&
            ackMode != Session.DUPS_OK_ACKNOWLEDGE && ackMode != Session.SESSION_TRANSACTED)
      {
         throw new DeploymentException("Invalid ack mode:" + ackMode);
      }
   }
   


   
   protected class Receiver extends AbstractServitor
   {
      private boolean failed; 
      
      private boolean stopping;
      
      public void stop()
      {
         stopping = true;
      }
      
      public void run()
      {
         try
         {
            log.info("Running receiver");
            
            Thread.sleep(rampDelay);
            
            Connection conn = getNextConnection();
          
            Session sess = conn.createSession(transacted, ackMode);
           
            MessageConsumer cons = sess.createConsumer(dest, selector, noLocal);

            long count = 0;
            
            while (true)
            {
            
               Message m = cons.receive(1000);     
                      
               if (m != null)
               {
                  count++;
               }
               
               if (count != 0 && count % 10 == 0)
               {
                  updateTotalCount(10);
                  count = 0;
               }
               
                             
               if (transacted)
               {
                  if (count % transactionSize == 0)
                  {
                     sess.commit();
                  }
               }                            
               
               if (stopping)
               {
                  break;
               }
            }
                        
            updateTotalCount(count);
            
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
   
}
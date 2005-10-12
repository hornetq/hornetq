
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
   

   public Servitor createServitor()
   {
      return new Receiver();
   }
   
   public String getName()
   {
      return "Receiver";
   }
   
   public ReceiverJob()
   {
      
   }
   
   public ReceiverJob(Element e) throws ConfigurationException
   {
      super(e);
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
   
   public void importXML(Element element) throws ConfigurationException
   {  
      super.importXML(element);
      
      this.ackMode = Integer.parseInt(MetadataUtils.getUniqueChildContent(element, "acknowledgement-mode"));
      this.subName = MetadataUtils.getOptionalChildContent(element, "durable-subscription-name", null);
      this.selector = MetadataUtils.getOptionalChildContent(element, "mesage-selector", null);
      this.noLocal = MetadataUtils.getOptionalChildBooleanContent(element, "no-local", false);
      this.asynch = MetadataUtils.getOptionalChildBooleanContent(element, "use-listener", false);
      
      if (ackMode != Session.AUTO_ACKNOWLEDGE && ackMode != Session.CLIENT_ACKNOWLEDGE &&
            ackMode != Session.DUPS_OK_ACKNOWLEDGE && ackMode != Session.SESSION_TRANSACTED)
      {
         throw new ConfigurationException("Invalid ack mode:" + ackMode);
      }
   }
   

   protected class Receiver extends AbstractServitor
   {
      
      public void run()
      {
         try
         {
            log.info("Running receiver");
            
            Connection conn = getNextConnection();
          
            Session sess = conn.createSession(transacted, ackMode);
           
            MessageConsumer cons = sess.createConsumer(dest, selector, noLocal);
            
            while (!stopping)
            {               
            
               Message m = cons.receiveNoWait();  
                      
               if (m != null)
               {
                  count++;
                  //log.info("Received message");
                  if (transacted)
                  {
                     if (count % transactionSize == 0)
                     {
                        sess.commit();
                     }
                  } 
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
   
   
   public void fillInResults(ResultPersistor persistor)
   {
      super.fillInResults(persistor);
      persistor.addValue("ackMode", this.ackMode);
      persistor.addValue("subName", this.subName);
      persistor.addValue("selector", selector);
      persistor.addValue("noLocal", this.noLocal);
      persistor.addValue("asynch", this.asynch);      
   }
   
}
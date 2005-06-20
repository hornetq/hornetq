/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import javax.jms.Session;
import javax.jms.IllegalStateException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.tx.AckInfo;
import org.jboss.logging.Logger;


/**
 * 
 * This interceptor handles JMS session state.
 *
 * In particular it stores un-acknowledged message information for a particular JMS session.
 * There should be one instance of this interceptor per JMS session
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 */
public class SessionInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = -8567252489464374932L;
   
   private static final Logger log = Logger.getLogger(SessionInterceptor.class);
   
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ----------------------------------
   
   public String getName()
   {
      return "SessionInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {      
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      String methodName = mi.getMethod().getName();
      
      if (log.isTraceEnabled()) log.trace("In SessionInterceptor: method is " + methodName);
      
      if ("acknowledgeSession".equals(methodName))
      {
         //Acknowledge all the messages received in this session
         if (log.isTraceEnabled()) { log.trace("acknowledgeSession called"); }
         
         //This only does anything if in client acknowledge mode
         int acknowledgmentMode =
            ((Integer)mi.getMetaData(JMSAdvisor.JMS, JMSAdvisor.ACKNOWLEDGMENT_MODE)).intValue();
         if (acknowledgmentMode != Session.CLIENT_ACKNOWLEDGE)
         {
            return null;
         }
                        
         Object Xid = mi.getMetaData(JMSAdvisor.JMS, JMSAdvisor.XID);
         
         if (Xid != null)
         {
            //Transacted session - so do nothing
            return null;
         }
         else
         {
            ArrayList unacked = getUnacknowledged(invocation);
            if (log.isTraceEnabled()) 
               log.trace("I have " + unacked.size() + " messages in the session to ack");
            Iterator iter = unacked.iterator();
            while (iter.hasNext())
            {
               AckInfo ackInfo = (AckInfo)iter.next();
               getDelegate(mi).acknowledge(ackInfo.messageID, ackInfo.receiverID);
            }
            unacked.clear();
            return null;
         }
      }     
      else if ("delivered".equals(methodName))
      {     
         String messageID = (String)mi.getArguments()[0];
         String receiverID = (String)mi.getArguments()[1];
         
         int acknowledgmentMode =
            ((Integer)mi.getMetaData(JMSAdvisor.JMS, JMSAdvisor.ACKNOWLEDGMENT_MODE)).intValue();
         
         Object Xid = mi.getMetaData(JMSAdvisor.JMS, JMSAdvisor.XID);
         
         if (Xid != null)
         {
            //Session is transacted - we just pass the ack on to the TransactionInterceptor
            if (log.isTraceEnabled()) log.trace("Session is transacted - passing ack on");
            getDelegate(mi).acknowledge(messageID, receiverID);
         }
         else if (acknowledgmentMode == Session.AUTO_ACKNOWLEDGE)
         {
            //Just acknowledge now
            if (log.isTraceEnabled()) log.trace("Auto-acking message");
            getDelegate(mi).acknowledge(messageID, receiverID);
         }
         else if (acknowledgmentMode == Session.DUPS_OK_ACKNOWLEDGE)
         {
            //TODO Lazy acks - for now we ack individually
            if (log.isTraceEnabled()) log.trace("Lazy acking message");
            getDelegate(mi).acknowledge(messageID, receiverID);
         }
         else if (acknowledgmentMode == Session.CLIENT_ACKNOWLEDGE)
         {
            if (log.isTraceEnabled()) log.trace("Client acknowledge so storing in unacked msgs");
            getUnacknowledged(invocation).add(new AckInfo(messageID, receiverID));
            if (log.isTraceEnabled()) log.trace("There are now " + getUnacknowledged(invocation).size() + " messages");
         }
         
         return null;
      }
      else if ("close".equals(methodName))
      {
         
         getUnacknowledged(invocation).clear();
         
         //need to rollback
         //getDelegate(mi).rollback();
                           
      }
      else if ("recover".equals(methodName))
      {
         Object Xid = mi.getMetaData(JMSAdvisor.JMS, JMSAdvisor.XID);
         if (Xid != null)
         {
            throw new IllegalStateException("Cannot recover a transacted session");
         }
         getUnacknowledged(invocation).clear();
         
         //Tell the server to redeliver any un-acked messages        
         getDelegate(mi).redeliver();
         return null;         
      }
      
      return invocation.invokeNext();
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }
   
   private SessionDelegate getDelegate(Invocation invocation)
   {
      return (SessionDelegate)getHandler(invocation).getDelegate();
   }
   
   private synchronized ArrayList getUnacknowledged(Invocation invocation)
   {
      if (log.isTraceEnabled()) log.trace("Getting unacknowledged messages");
      
      
      //Sanity check - this will throw an exception if the invocation is not a SessionDelegate
        getDelegate(invocation); 
      
      
      //TODO If we have a lot of unacked messages building up for the session
      //we need a better way to store them than just an ArrayList.
      //We risk running out of memory otherwise
      JMSInvocationHandler handler = getHandler(invocation);
      ArrayList unacked =
         (ArrayList)handler.getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.UNACKED);
      if (unacked == null)
      {
         if (log.isTraceEnabled()) log.trace("Creating new list");
         unacked = new ArrayList();
         handler.getMetaData().addMetaData(JMSAdvisor.JMS, JMSAdvisor.UNACKED, unacked);
      }
      return unacked;
   }  
   
   // Inner Classes -------------------------------------------------
   
   

	   		
   
	
	
}


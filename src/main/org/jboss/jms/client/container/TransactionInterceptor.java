/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.io.Serializable;

import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.TransactionInProgressException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossXAResource;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManager.LocalTxXid;
import org.jboss.logging.Logger;

/**
 * This interceptor handles transaction related logic
 * 
 * There should be one instance of this interceptor per instance of Connection, Session
 * and Producer
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 */
public class TransactionInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private final static long serialVersionUID = -34322737839473785L;
   
   private static final Logger log = Logger.getLogger(TransactionInterceptor.class);
   
   // Attributes ----------------------------------------------------
   
   protected ResourceManager rm;
   
   protected JBossXAResource xaResource;
   
   protected boolean transacted;
   
   protected boolean XA;     
   
   //protected Object txID;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   // Interceptor implementation ------------------------------------
   
   public String getName()
   {
      return "TransactionInterceptor";
   }
   
   public Object invoke(Invocation invocation) throws Throwable
   {
      if (!(invocation instanceof MethodInvocation))
      {
         return invocation.invokeNext();
      }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();
      
      if (log.isTraceEnabled()) { log.trace("In TransactionInterceptor: method is " + methodName); }
      
      if ("createSessionDelegate".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("createSessionDelegate"); }
         
         SessionDelegate sd = (SessionDelegate)invocation.invokeNext();
         
         boolean transacted = ((Boolean)mi.getArguments()[0]).booleanValue();
         boolean isXA = ((Boolean)mi.getArguments()[2]).booleanValue();
         
         if (log.isTraceEnabled())
         {
            log.trace("transacted:" + transacted + ", xa:" + isXA);
         }
         
         sd.setTransacted(transacted);
         sd.setXA(isXA);
         if (transacted)
         {            
            ResourceManager theRm = ((ConnectionDelegate)this.getDelegate(mi)).getResourceManager();
            
            if (log.isTraceEnabled()) { log.trace("Transacted so creating XAResource"); }
            
            JBossXAResource theResource = new JBossXAResource(theRm);
                        
            //Create a local tx                     										
            Object Xid = theRm.createLocalTx();
            
            if (log.isTraceEnabled()) { log.trace("Created local tx"); }
            
            theResource.setCurrentTxID(Xid); 
            
            sd.setXAResource(theResource);
            sd.setResourceManager(theRm);
         }            
         return sd;
      }         
      else if ("commit".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("commit"); }
         
         //SessionDelegate sessDelegate = (SessionDelegate)this.getDelegate(mi);
         
         if (!this.transacted)
         {
            throw new IllegalStateException("Cannot commit a non-transacted session");
         }
         if (this.XA)
         {
            throw new TransactionInProgressException("Cannot call commit on an XA session");
         }
         
         try
         {
            rm.commitLocal((LocalTxXid)xaResource.getCurrentTxID());
         }
         finally
         {
            //Start new local tx
            Object xid = rm.createLocalTx();
            xaResource.setCurrentTxID(xid);
         }
         
         return null;
      }
      else if ("rollback".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("rollback"); }
         
         SessionDelegate sessDelegate = (SessionDelegate)this.getDelegate(mi);
         if (!this.transacted)
         {
            throw new IllegalStateException("Cannot rollback a non-transacted session");
         }
         if (this.XA)
         {
            throw new TransactionInProgressException("Cannot call rollback on an XA session");
         }
         
         try
         {
            rm.rollbackLocal((LocalTxXid)xaResource.getCurrentTxID());
         }
         finally
         {
            //Start new local tx
            Object xid = rm.createLocalTx();
            xaResource.setCurrentTxID(xid);
         } 
         
         //Rollback causes redelivery of messages
         sessDelegate.redeliver();
         return null;				
      }
      else if ("send".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("send"); }
         
         JMSInvocationHandler handler = getHandler(invocation).getParent();
         
         SessionDelegate sessDelegate = (SessionDelegate)handler.getDelegate();
         
         JBossXAResource jbXAResource = (JBossXAResource)sessDelegate.getXAResource();
         
         ResourceManager theRM = sessDelegate.getResourceManager();
         
         if (log.isTraceEnabled()) { log.trace("XAResource is: " + jbXAResource); }
         
         if (sessDelegate.getTransacted())
         {
            //Session is transacted - so we add message to tx instead of sending now
            
            Message m = (Message)mi.getArguments()[1];			
            theRM.addMessage(jbXAResource.getCurrentTxID(), m);
            
            //And we don't invoke any further interceptors in the stack
            return null;               
         }
         else
         {
            if (log.isTraceEnabled()) { log.trace("Session is not transacted, sending message now"); }
         }
      }
      else if ("preDeliver".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("predeliver"); }
         
         //SessionDelegate sessDelegate = (SessionDelegate)this.getDelegate(mi);
         if (!this.transacted)
         {
            //Not transacted - do nothing - ack will happen when delivered() is called
            if (log.isTraceEnabled()) { log.trace("Session is not transacted"); }
            return null;
         }
         else 
         {
            String messageID = (String)mi.getArguments()[0];
            String receiverID = (String)mi.getArguments()[1];
            
            if (log.isTraceEnabled())
            {
               log.trace("XAresource: " + xaResource);
            }
            
            rm.addAck(xaResource.getCurrentTxID(), new AckInfo(messageID, receiverID));
            
            return null;
         }
      }  
      else if ("getXAResource".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("getXAResource"); }
         
         return xaResource;
      }
      else if ("setXAResource".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("setXAResource"); }
         JBossXAResource resource = (JBossXAResource)mi.getArguments()[0];
         this.xaResource = resource;
         return null;
      }
      else if ("getResourceManager".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("getResourceManager"); }
         
         return this.rm;
      }
      else if ("setResourceManager".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("setResourceManager"); }
         ResourceManager theRM = (ResourceManager)mi.getArguments()[0];
         this.rm = theRM;
         return null;
      }
      else if ("getTransacted".equals(methodName))
      {
         //SessionState state = getSessionState(mi);
         return new Boolean(transacted);
      }
      else if ("getXA".equals(methodName))
      {
         //SessionState state = getSessionState(mi);
         return new Boolean(XA);
      }
      else if ("setTransacted".equals(methodName))
      {
         this.transacted = ((Boolean)mi.getArguments()[0]).booleanValue();
         return null;
      }
      else if ("setXA".equals(methodName))
      {
         this.XA = ((Boolean)mi.getArguments()[0]).booleanValue();
         return null;
      }
      
      
      return invocation.invokeNext();            
   }
   
   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------
   
   // Private --------------------------------------------------------
   
   
   //Helper methods
   
   private Object getDelegate(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler().getDelegate();
   }
    
   private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }
   
      
   // Inner Classes --------------------------------------------------
   
}



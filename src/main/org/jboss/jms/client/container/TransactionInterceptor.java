/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;


import java.lang.reflect.Proxy;
import java.io.Serializable;

import javax.jms.Message;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.logging.Logger;


/**
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 */
public class TransactionInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -34322737839473785L;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(TransactionInterceptor.class);
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "TransactionInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
      
         String methodName = mi.getMethod().getName();
         
         if ("createSessionDelegate".equals(methodName))
         {
            log.debug("createSessionDelegate");
            
            SessionDelegate sd = (SessionDelegate)invocation.invokeNext();
            
            //If we get here, the sesion has been created
            
            Boolean transacted = (Boolean)mi.getArguments()[0];
            
            log.debug("Transacted: " + transacted);
            
            if (transacted == null)
               throw new IllegalStateException("Cannot find transacted argument");

            if (transacted.booleanValue())
            {
               log.debug("Session is transacted");
               
               //Session has been created and is transacted so we start a tx               
               
               Object Xid = ResourceManager.instance().createLocalTx();
               
               log.debug("Created tx");
               
               //And set it on the meta-data of the returned delegate's invocation handler
               JMSInvocationHandler sessionHandler = getHandler(sd);               
               setMetaData(sessionHandler, JMSAdvisor.XID, Xid);               
               
               log.debug("Added Xid to meta-data");
            }
            
            return sd;
         }
         else if ("close".equals(methodName))
         {
            //Rollback the session
            
            //Make sure this invocation is for the Session since other delegates have a close() method
            
            //TODO
         }
         else if ("commit".equals(methodName))
         {
            log.debug("commit");
            Object Xid = getMetaData(mi, JMSAdvisor.XID);                        
            SessionDelegate sd = (SessionDelegate)getDelegate(mi);            
            Object newXid = ResourceManager.instance().commit(Xid, sd);
            setMetaData(mi, JMSAdvisor.XID, newXid);                      
         }
         else if ("rollback".equals(methodName))
         {
            log.debug("rollback");
            Object Xid = mi.getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.XID);                     
            Object newXid = ResourceManager.instance().rollback(Xid);
            setMetaData(mi, JMSAdvisor.XID, newXid); 
         }
         else if ("send".equals(methodName))
         {
            if (log.isTraceEnabled()) { log.trace("send"); }

            JMSInvocationHandler sessionInvocationHandler = getHandler(invocation).getParent();

            if (log.isTraceEnabled()) { log.trace("sessionInvocationHandler: " + sessionInvocationHandler); }

            Object Xid = getMetaData(sessionInvocationHandler, JMSAdvisor.XID);
            if (Xid != null)
            {
               //Session is transacted - so we add message to tx instead of sending now
               if (log.isTraceEnabled()) { log.trace("Session is transacted, storing mesage until commit"); }
               Message m = (Message)mi.getArguments()[0];
               ResourceManager.instance().addMessage(Xid, m);

               //And we don't invoke any further interceptors in the stack
               return null;               
            }
            else
            {
               if (log.isTraceEnabled()) { log.trace("Session is not transacted, sending message now"); }
            }
         }
         else if ("recover".equals(methodName))
         {
            //TODO
            //I'm not sure whether this needs to be handled here or in the SesionInterceptor
         }
         
      }
               
      return invocation.invokeNext();            
   }

   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------
         
   
   //Helper methods
   
   private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }
   
   private Object getDelegate(Invocation invocation)
   {
      return getHandler(invocation).getDelegate();
   }
   
   private JMSInvocationHandler getHandler(Object delegate)
   {
      return (JMSInvocationHandler)Proxy.getInvocationHandler(delegate);
   }
   
   private Object getMetaData(JMSInvocationHandler handler, Object attribute)
   {
      return handler.getMetaData().getMetaData(JMSAdvisor.JMS, attribute);
   }
   
   private Object getMetaData(MethodInvocation mi, Object attribute)
   {
      return mi.getMetaData().getMetaData(JMSAdvisor.JMS, attribute);
   }
   
   private void setMetaData(JMSInvocationHandler handler, Object attribute, Object value)
   {
      handler.getMetaData().addMetaData(JMSAdvisor.JMS, attribute, value, PayloadKey.AS_IS);
   }
   
   private void setMetaData(Invocation invocation, Object attribute, Object value)
   {
      getHandler(invocation).getMetaData()
         .addMetaData(JMSAdvisor.JMS, attribute, value, PayloadKey.AS_IS);
   }
   
   
   // Inner Classes --------------------------------------------------

}



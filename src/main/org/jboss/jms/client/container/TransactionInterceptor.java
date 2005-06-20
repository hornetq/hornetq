/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;


import java.io.Serializable;
import java.lang.reflect.Proxy;

import javax.jms.Message;
import javax.jms.IllegalStateException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.logging.Logger;


/**
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 */
public class TransactionInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -34322737839473785L;
   
   private static final Logger log = Logger.getLogger(TransactionInterceptor.class);

   // Attributes ----------------------------------------------------

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
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
      
         String methodName = mi.getMethod().getName();
         
			if ("createConnectionDelegate".equals(methodName))
			{
				if (log.isTraceEnabled()) log.trace("creating resource manager");
				//Create new ResourceManager and add to meta-data for invocation handler
				//for connection
				ConnectionDelegate connectionDelegate = (ConnectionDelegate)invocation.invokeNext();
				ResourceManager rm = new ResourceManager(connectionDelegate);
				JMSInvocationHandler handler = (JMSInvocationHandler)Proxy.getInvocationHandler(connectionDelegate);				
				handler.getMetaData().addMetaData(JMSAdvisor.JMS, JMSAdvisor.RESOURCE_MANAGER, rm, PayloadKey.TRANSIENT);
				return connectionDelegate;
			}			
			else if ("createSessionDelegate".equals(methodName))
         {
            if (log.isTraceEnabled()) log.trace("createSessionDelegate");
            
            SessionDelegate sd = (SessionDelegate)invocation.invokeNext();
            
            //If we get here, the session has been created
            
            Boolean transacted = (Boolean)mi.getArguments()[0];
            
				if (log.isTraceEnabled()) log.trace("Transacted: " + transacted);
            
            if (transacted == null)
               throw new IllegalStateException("Cannot find transacted argument");

            if (transacted.booleanValue())
            {
					if (log.isTraceEnabled()) log.trace("Session is transacted");
               
               //Session has been created and is transacted so we start a tx               
               
					JMSInvocationHandler handler = ((JMSMethodInvocation)invocation).getHandler();
					
					ResourceManager rm =
						(ResourceManager)handler.getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.RESOURCE_MANAGER);	
					
               Object Xid = rm.createLocalTx();
               
					if (log.isTraceEnabled()) log.trace("Created tx");
               
               //And set it on the meta-data of the returned delegate's invocation handler
               JMSInvocationHandler sessionHandler = getHandler(sd);               
               setMetaData(sessionHandler, JMSAdvisor.XID, Xid);               
               
					if (log.isTraceEnabled()) log.trace("Added Xid to meta-data");
            }
            
            return sd;
         }         
         else if ("commit".equals(methodName))
         {
				if (log.isTraceEnabled()) log.trace("commit");
				
            Object Xid = getMetaData(mi, JMSAdvisor.XID);                        
            //SessionDelegate sd = (SessionDelegate)getDelegate(mi);  
				ResourceManager rm = (ResourceManager)getHandler(invocation).getParent().getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.RESOURCE_MANAGER);
            Object newXid = rm.commit(Xid);
            setMetaData(mi, JMSAdvisor.XID, newXid);   
				
				return null;
         }
         else if ("rollback".equals(methodName))
         {
				if (log.isTraceEnabled()) log.trace("rollback");
            Object Xid = mi.getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.XID);   
				ResourceManager rm = (ResourceManager)getHandler(invocation).getParent().getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.RESOURCE_MANAGER);
            Object newXid = rm.rollback(Xid);
            setMetaData(mi, JMSAdvisor.XID, newXid); 
				
				//Rollback causes redelivery of messages
				SessionDelegate sd = (SessionDelegate)getDelegate(mi);
				sd.redeliver();
				return null;				
         }
         else if ("send".equals(methodName))
         {
            if (log.isTraceEnabled()) log.trace("send"); 

            JMSInvocationHandler sessionInvocationHandler = getHandler(invocation).getParent();

            if (log.isTraceEnabled()) { log.trace("sessionInvocationHandler: " + sessionInvocationHandler); }

            Object Xid = getMetaData(sessionInvocationHandler, JMSAdvisor.XID);
            if (Xid != null)
            {
               //Session is transacted - so we add message to tx instead of sending now
               if (log.isTraceEnabled())  log.trace("Session is transacted, storing mesage until commit");
               Message m = (Message)mi.getArguments()[0];
					ResourceManager rm = (ResourceManager)getHandler(invocation).getParent().getParent().getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.RESOURCE_MANAGER);
					rm.addMessage(Xid, m);

               //And we don't invoke any further interceptors in the stack
               return null;               
            }
            else
            {
               if (log.isTraceEnabled()) { log.trace("Session is not transacted, sending message now"); }
            }
         }
         else if ("acknowledge".equals(methodName))
			{
				if (log.isTraceEnabled()) log.trace("acknowledge");
				
				Object Xid = mi.getMetaData(JMSAdvisor.JMS, JMSAdvisor.XID);
				
				if (Xid == null)
				{
					//Not transacted - this goes through to the ServerSessionDelegate
					if (log.isTraceEnabled()) { log.trace("Session is not transacted, acking message now"); }
				}
				else 
				{
					String messageID = (String)mi.getArguments()[0];
					String receiverID = (String)mi.getArguments()[1];
					
					ResourceManager rm = (ResourceManager)getHandler(invocation).getParent().getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.RESOURCE_MANAGER);
					
					rm.addAck(Xid, new AckInfo(messageID, receiverID));
					
					return null;
				}
			}                  
      }
               
      return invocation.invokeNext();            
   }

   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------
         
   
   //Helper methods
	
	/*
	private ResourceManager getResourceManager(Invocation invocation)
	{
		return (ResourceManager)getHandler(invocation).getParent().getMetaData().getMetaData(JMSAdvisor.JMS, JMSAdvisor.RESOURCE_MANAGER);		
	}
   
   */
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



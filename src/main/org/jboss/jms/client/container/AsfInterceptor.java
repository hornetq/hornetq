/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;


import java.io.Serializable;
import java.util.LinkedList;

import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ServerSessionPool;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.logging.Logger;
import org.jgroups.protocols.JMS;


/**
 * An interceptor the Application Server Facilities for concurrent processing of a consumer's
 * messages.
 * 
 * @see JMS 1.1 spec. section 8.2
 * 
 * Important! There should be *one instance* of this interceptor per instance of Connection or Session
 * on which this interceptor lives.
 *  
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 */
public class AsfInterceptor
   implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -2616170810812856267L;

   private static final Logger log = Logger.getLogger(AsfInterceptor.class);

   // Attributes ----------------------------------------------------

   //The list of messages that get processed on a call to run()
   protected LinkedList msgs = new LinkedList();
   
   //The Session listener - the distinguished message listener
   protected MessageListener sessionListener;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "AsfInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (!(invocation instanceof MethodInvocation))
      {
         return invocation.invokeNext();
      }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();
      
      if ("setMessageListener".equals(methodName))
      {  
         if (log.isTraceEnabled()) { log.trace("setMessageListener"); }
         //Invoked from Session
         MessageListener listener = (MessageListener)mi.getArguments()[0];
         if (listener == null)
         {
            throw new IllegalStateException("Cannot set a null MessageListener on the session");
         }
         sessionListener = listener;
         return null;
      }
      else if ("getMessageListener".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("getMessageListener"); }
         //Invoked from Session
         return sessionListener;
      }
      else if ("createConnectionConsumer".equals(methodName))
      {         
         //Invoked from Connection
         if (log.isTraceEnabled()) { log.trace("createConnectionConsumer"); }
         Destination dest = (Destination)mi.getArguments()[0];
         String subscriptionName = (String)mi.getArguments()[1];
         String messageSelector = (String)mi.getArguments()[2];
         ServerSessionPool sessionPool = (ServerSessionPool)mi.getArguments()[3];
         int maxMessages = ((Integer)mi.getArguments()[4]).intValue();
         
         ConnectionConsumer cc =
            new JBossConnectionConsumer((ConnectionDelegate)getDelegate(mi), dest, subscriptionName,
                                        messageSelector, sessionPool, maxMessages);        
         return cc;         
      }
      else if ("addAsfMessage".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("addAsfMessage"); }
         
         //Invoked from Sessin
         
         //Load the session with a message to be processed during a subsequent call to run()
         
         Message m = (Message)mi.getArguments()[0];
         String receiverID = (String)mi.getArguments()[1];
         
         if (m == null)
         {
            throw new IllegalStateException("Cannot add a null message to the session");
         }
         if (receiverID == null)
         {
            throw new IllegalStateException("Cannot add a message without specifying receiverID");
         }
         
         AsfMessageHolder holder = new AsfMessageHolder();
         holder.msg = m;
         holder.receiverID = receiverID;
         
         synchronized (msgs)
         {
            msgs.add(holder);
         }
         
         return null;
      }
      else if ("run".equals(methodName))
      {               
         if (log.isTraceEnabled()) { log.trace("run"); }
         SessionDelegate del = (SessionDelegate)getDelegate(mi);
         synchronized (msgs)
         {
            while (msgs.size() > 0)
            {
               AsfMessageHolder holder = (AsfMessageHolder)msgs.removeFirst();
               
               del.preDeliver(holder.msg.getJMSMessageID(), holder.receiverID);
               
               sessionListener.onMessage(holder.msg);
                              
               del.postDeliver(holder.msg.getJMSMessageID(), holder.receiverID);               
            }
         }
         return null;
      }
      return invocation.invokeNext();
               
   }
   
   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------
   
   private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }
   
   private Object getDelegate(Invocation invocation)
   {
      return getHandler(invocation).getDelegate();
   }
   

   // Inner Classes --------------------------------------------------
   
   protected static class AsfMessageHolder
   {
      Message msg;
      String receiverID;
   }
}

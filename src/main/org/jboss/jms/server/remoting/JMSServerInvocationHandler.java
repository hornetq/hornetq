/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.remoting;

import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvokerCallbackHandler;
import org.jboss.remoting.ServerInvokerCallbackHandler;
import org.jboss.logging.Logger;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.Dispatcher;
import org.jboss.jms.server.container.JMSAdvisor;

import javax.management.MBeanServer;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSServerInvocationHandler implements ServerInvocationHandler
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSServerInvocationHandler.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private ServerInvoker invoker;
   private MBeanServer server;

   protected Map callbackHandlers;

   // Constructors --------------------------------------------------

   public JMSServerInvocationHandler()
   {
      callbackHandlers = new HashMap();
   }

   // ServerInvocationHandler ---------------------------------------

   public void setMBeanServer(MBeanServer server)
   {
      this.server = server;
   }

   public void setInvoker(ServerInvoker invoker)
   {
      this.invoker = invoker;
   }

   public Object invoke(InvocationRequest invocation) throws Throwable
   {
      Invocation i =(Invocation)invocation.getParameter();
      String s = (String)i.getMetaData(JMSAdvisor.JMS, JMSAdvisor.REMOTING_SESSION_ID);
      Object callbackHandler = null;
      synchronized(callbackHandlers)
      {
         callbackHandler = callbackHandlers.get(s);
      }
      if (callbackHandler != null)
      {
         i.getMetaData().addMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER, callbackHandler);
      }
      return Dispatcher.singleton.invoke(i);
   }

   public void addListener(InvokerCallbackHandler callbackHandler)
   {
      log.debug("adding callback handler: " + callbackHandler);
      if (callbackHandler instanceof ServerInvokerCallbackHandler)
      {
         ServerInvokerCallbackHandler h = (ServerInvokerCallbackHandler)callbackHandler;
         String id = h.getId();
         synchronized(callbackHandlers)
         {
            if (callbackHandlers.containsKey(id))
            {
               String msg = "The remoting client " + id + " already has a callback handler";
               log.error(msg);
               throw new RuntimeException(msg);
            }
            callbackHandlers.put(id, h);
         }
      }
      else
      {
         throw new RuntimeException("Do not know how to use callback handler " + callbackHandler);
      }
   }

   public void removeListener(InvokerCallbackHandler callbackHandler)
   {
      log.debug("removing callback handler: " + callbackHandler);
      synchronized(callbackHandlers)
      {
         for(Iterator i = callbackHandlers.keySet().iterator(); i.hasNext();)
         {
            Object key = i.next();
            if (callbackHandler.equals(callbackHandlers.get(key)))
            {
               callbackHandlers.remove(key);
               return;
            }
         }
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}

/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting.util;

import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;
import org.jboss.logging.Logger;
import org.jboss.test.thirdparty.remoting.util.CallbackTrigger;

import javax.management.MBeanServer;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.Channel;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemotingTestSubsystem implements ServerInvocationHandler, Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 5457454557215715L;

   private static final Logger log = Logger.getLogger(RemotingTestSubsystem.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private Channel invocationHistory;
   private List callbackListeners;

   // Constructors ---------------------------------------------------------------------------------

   public RemotingTestSubsystem()
   {
      invocationHistory = new LinkedQueue();
      callbackListeners = new ArrayList();
   }

   // ServerInvocationHandler implementation -------------------------------------------------------

   public void setMBeanServer(MBeanServer server)
   {
   }

   public void setInvoker(ServerInvoker invoker)
   {
   }

   public Object invoke(InvocationRequest invocation) throws Throwable
   {
      log.debug(this + " received " + invocation);

      Object parameter = invocation.getParameter();

      if ("ignore".equals(parameter))
      {
         // used in stress tests, do not accumulate record the invocation in history, since the
         // client is goint to send a lot of them ....
         log.debug(this + " ignoring invocation");
         return null;
      }

      invocationHistory.put(invocation);

      if (parameter instanceof CallbackTrigger)
      {
         Callback callback = new Callback(((CallbackTrigger)parameter).getPayload());

         // seding a callback asynchronously
         pushToClient(callback, false);
      }

      return null;
   }

   public synchronized void addListener(InvokerCallbackHandler callbackHandler)
   {
      callbackListeners.add(callbackHandler);
   }

   public synchronized void removeListener(InvokerCallbackHandler callbackHandler)
   {
      callbackListeners.remove(callbackHandler);
   }

   // Public ---------------------------------------------------------------------------------------

   public InvocationRequest getNextInvocation(long timeout) throws InterruptedException
   {
      return (InvocationRequest)invocationHistory.poll(timeout);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private synchronized void pushToClient(Callback callback, boolean sendSynchronously)
   {
      for(Iterator i = callbackListeners.iterator(); i.hasNext(); )
      {
         ServerInvokerCallbackHandler h = (ServerInvokerCallbackHandler)i.next();
         try
         {
            if (sendSynchronously)
            {
               log.debug("pushing synchronous callback to " + h);
               h.handleCallback(callback);

            }
            else
            {
               log.debug("pushing asynchronous callback to " + h);
               h.handleCallbackOneway(callback);
            }
         }
         catch(Exception e)
         {
            log.error("Sending callback failed", e);
         }
      }
   }

   // Inner classes --------------------------------------------------------------------------------
}

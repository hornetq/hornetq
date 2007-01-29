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
import org.jboss.test.thirdparty.remoting.SocketTransportCausalityTest;
import org.jboss.logging.Logger;

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

   /**
    * Very quick and dirty method. Don't try it at home. Needed it because some InvocationRequests
    * (even if the class is declared Serializable) contain request and response payloads which are
    * not, so I am having trouble sending them over wire back to the client.
    */
   private static InvocationRequest dirtyCopy(InvocationRequest source)
   {
      return new InvocationRequest(source.getSessionId(),
                                   source.getSubsystem(),
                                   source.getParameter(),
                                   null,
                                   null,
                                   source.getLocator());
   }

   // Attributes -----------------------------------------------------------------------------------

   private Channel invocationHistory;
   private List callbackListeners;
   
   private int[] counters = new int[10];
   
   private boolean failed;

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

      final Object parameter = invocation.getParameter();

      if ("ignore".equals(parameter))
      {
         // used in stress tests, do not accumulate record the invocation in history, since the
         // client is goint to send a lot of them ....
         log.debug(this + " ignoring invocation");
         return null;
      }
      
      if (parameter instanceof SocketTransportCausalityTest.SimpleInvocation)
      {
         SocketTransportCausalityTest.SimpleInvocation inv = (SocketTransportCausalityTest.SimpleInvocation)parameter;
         
         synchronized (this)
         {            
            int clientNum = inv.clientNumber;
            
            int lastCount = this.counters[clientNum];
            
            log.trace("Received client " + clientNum + " num " + inv.num);
            
            if (inv.num != lastCount + 1)
            {
               //Failed - out of sequence
               failed = true;
               
               log.trace("Failed!!!! out of sequence");
            }
            
            counters[clientNum] = inv.num;
            
            return null;
         }
         
      }

      invocationHistory.put(dirtyCopy(invocation));

      if (parameter instanceof OnewayCallbackTrigger)
      {
         // send all oneway invocations from a different thread to avoid blocking the worker thread
         // that has to return and write a response on the wire

         new Thread(new Runnable()
         {
            public void run()
            {
               OnewayCallbackTrigger t = (OnewayCallbackTrigger)parameter;
               String payload = t.getPayload();
               long[] triggerTimes = t.getTriggerTimes();

               for(int i = 0; i < triggerTimes.length; i++)
               {
                  Callback callback = new Callback(payload + (i != 0 ? Integer.toString(i) : ""));

                  try
                  {
                     Thread.sleep(triggerTimes[i]);
                  }
                  catch(InterruptedException e)
                  {
                     log.error("interrupted", e);
                     return;
                  }

                  // seding a callback asynchronously
                  pushToClient(callback, false);
               }
            }
         }, "Oneway Invoker Thread").start();

         log.debug(this + " started a new oneway invoker thread");

      }

      return null;
   }

   public void addListener(InvokerCallbackHandler callbackHandler)
   {
      synchronized(callbackListeners)
      {
         callbackListeners.add(callbackHandler);
      }
   }

   public void removeListener(InvokerCallbackHandler callbackHandler)
   {
      synchronized(callbackListeners)
      {
         callbackListeners.remove(callbackHandler);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public InvocationRequest getNextInvocation(long timeout) throws InterruptedException
   {
      return (InvocationRequest)invocationHistory.poll(timeout);
   }
   
   public boolean isFailed()
   {
      synchronized (this)
      {
         return failed;
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private synchronized void pushToClient(Callback callback, boolean sendSynchronously)
   {
      // make a copy to avoid ConcurrentModificationException
      List callbackListenersCopy;
      synchronized(callbackListeners)
      {
         callbackListenersCopy = new ArrayList(callbackListeners);
      }

      for(Iterator i = callbackListenersCopy.iterator(); i.hasNext(); )
      {
         ServerInvokerCallbackHandler h = (ServerInvokerCallbackHandler)i.next();
         try
         {
            if (sendSynchronously)
            {
               log.debug("pushing synchronous callback " + callback + " to " + h);
               h.handleCallback(callback);
            }
            else
            {
               log.debug("pushing asynchronous callback " + callback + " to " + h);
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

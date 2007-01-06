/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.logging.Logger;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.HierarchicalState;
import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.Sync;

import java.util.List;
import java.util.ArrayList;

/**
 * An interceptor that acts as a failover valve: it allows all invocations to go through as long
 * as there is no failover in progress (valve is open), and holds all invocations while client-side
 * failover is taking place (valve is closed). The interceptor fields org.jboss.jms.client.Valve's
 * method calls.
 *
 * It is a PER_INSTANCE interceptor.
 *
 * An instance of this interceptor must guard access to each connection, session, producer, consumer
 * and queue browser delegate.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class FailoverValveInterceptor implements Interceptor
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(FailoverValveInterceptor.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private DelegateSupport delegate;
   private HierarchicalState state;
   private volatile boolean valveOpen;

   private ReadWriteLock rwLock;

   // the number of threads currently "penetrating" the open valve
   private int activeThreadsCount;

   // only for tracing
   private List activeMethods;

   // Constructors ---------------------------------------------------------------------------------

   public FailoverValveInterceptor()
   {
      valveOpen = true;
      activeThreadsCount = 0;
      rwLock = new WriterPreferenceReadWriteLock();
      if (trace)
      {
         activeMethods = new ArrayList();
      }
   }

   // Interceptor implemenation --------------------------------------------------------------------

   public String getName()
   {
      return "FailoverValveInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      // maintain a reference to the delegate that sends invocation through this interceptor. It
      // makes sense, since it's an PER_INSTANCE interceptor.
      if (delegate == null)
      {
         delegate = (DelegateSupport)invocation.getTargetObject();
         state = delegate.getState();
      }

      String methodName = ((MethodInvocation)invocation).getMethod().getName();
      Sync writeLock =  rwLock.writeLock();
      Sync readLock = rwLock.readLock();

      if("closeValve".equals(methodName))
      {
         if (!valveOpen)
         {
            // valve already closed, this is a noop
            log.warn(this + " already closed!");
            return null;
         }

         state.closeChildrensValves();

         boolean acquired = false;

         while(!acquired)
         {
            try
            {
               acquired = writeLock.attempt(500);
            }
            catch(InterruptedException e)
            {
               // OK
            }

            if (!acquired)
            {
               log.debug(this + " failed to close");
            }
         }

         valveOpen = false;

         log.debug(this + " has been closed");

         return null;
      }
      else if("openValve".equals(methodName))
      {
         if (valveOpen)
         {
            // valve already open, this is a noop
            log.warn(this + " already open!");
            return null;
         }

         state.openChildrensValves();

         writeLock.release();
         valveOpen = true;

         log.debug(this + " has been opened");

         return null;
      }
      else if("isValveOpen".equals(methodName))
      {
         if (valveOpen)
         {
            return Boolean.TRUE;
         }
         else
         {
            return Boolean.FALSE;
         }
      }
      else if("getActiveThreadsCount".equals(methodName))
      {
         return new Integer(activeThreadsCount);
      }

      // attempt to grab the reader's lock and go forward

      boolean exempt = false;

      try
      {
         exempt = isInvocationExempt(methodName);

         if (!exempt)
         {
            boolean acquired = false;

            while(!acquired)
            {
               try
               {
                  acquired = readLock.attempt(500);
               }
               catch(InterruptedException e)
               {
                  // OK
               }

               if (trace && !acquired ) { log.trace(methodName + "() trying to pass through " + this); }
            }
         }

         synchronized(this)
         {
            activeThreadsCount++;
            if (trace)
            {
               activeMethods.add(methodName);
            }
         }

         if (trace) { log.trace(this + " allowed " + (exempt ? "exempt" : "") + " method " + methodName + "() to pass through"); }

         return invocation.invokeNext();
      }
      finally
      {
         if (!exempt)
         {
            readLock.release();
         }

         synchronized(this)
         {
            activeThreadsCount--;
            if (trace)
            {
               activeMethods.remove(methodName);
            }
         }

      }
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "FailoverValve." +
         (delegate == null ? "UNITIALIZED" : delegate.toString()) +
         (valveOpen ? "[OPEN(" + activeThreadsCount +
            (trace ? " " + activeMethods.toString() : "") + ")]":"[CLOSED]");
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private boolean isInvocationExempt(String methodName)
   {
      return "recoverDeliveries".equals(methodName);
   }

   // Inner classes --------------------------------------------------------------------------------
}

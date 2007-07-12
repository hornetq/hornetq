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
import org.jboss.jms.client.FailoverCommandCenter;
import org.jboss.jms.client.FailoverValve2;
import org.jboss.jms.client.FailureDetector;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.logging.Logger;

/**
 * An interceptor that acts as a failover valve: it allows all invocations to go through as long
 * as there is no failover in progress (valve is open), and holds all invocations while client-side
 * failover is taking place (valve is closed). The interceptor is also a failover detector, in that
 * it catches "failure-triggering" exceptions, and notifies the failover command center.
 *
 * The interceptor fields org.jboss.jms.client.Valve's method calls.
 *
 * It is a PER_INSTANCE interceptor.
 *
 * An instance of this interceptor must guard access to each connection, session, producer, consumer
 * and queue browser delegate.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class FailoverValveInterceptor implements Interceptor, FailureDetector
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(FailoverValveInterceptor.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private DelegateSupport delegate;

   // We need to cache connectionState here.
   // IMPORTANT - We must not cache the fcc or valve since these need to be replaced when failover
   //             occurs and if we cache them we wil end up using the old ones.
   private ConnectionState connectionState;

   // Constructors ---------------------------------------------------------------------------------

   // Interceptor implemenation --------------------------------------------------------------------

   public String getName()
   {
      return "FailoverValveInterceptor";
   }
   
   public Object invoke(Invocation invocation) throws Throwable
   {      
      // maintain a reference to connectionState, so we can ensure we have already tested for fcc.
      // As fcc can be null on non-clustered connections we have to cache connectionState instead
      if (connectionState == null)
      {
         delegate = (DelegateSupport)invocation.getTargetObject();

         HierarchicalState hs = delegate.getState();
         while (hs != null && !(hs instanceof ConnectionState))
         {
            hs = hs.getParent();
         }
         
         connectionState = (ConnectionState)hs;
      }
      
      FailoverCommandCenter fcc = connectionState.getFailoverCommandCenter();
            
      // non clustered, send the invocation forward
      if (fcc == null)
      {
         return invocation.invokeNext();
      }
      
      FailoverValve2 valve = fcc.getValve();
      
      JMSRemotingConnection remotingConnection = null;
      String methodName = ((MethodInvocation)invocation).getMethod().getName();

      boolean left = false;
      
      try
      {
         valve.enter();

         // it's important to retrieve the remotingConnection while inside the Valve
         remotingConnection = fcc.getRemotingConnection();
         return invocation.invokeNext();
      }
      catch (MessagingNetworkFailureException e)
      {
         valve.leave();
         left = true;
         
         log.debug(this + " detected network failure, putting " + methodName +
         "() on hold until failover completes");
      
         fcc.failureDetected(e, this, remotingConnection);
         
         // Set retry flag as true on send() and sendTransaction()
         // more details at http://jira.jboss.org/jira/browse/JBMESSAGING-809

         if (invocation.getTargetObject() instanceof ClientSessionDelegate &&
            (methodName.equals("send") || methodName.equals("sendTransaction")))
         {
            log.trace(this + " caught " + methodName + "() invocation, enabling check for duplicates");

            Object[] arguments = ((MethodInvocation)invocation).getArguments();
            arguments[1] = Boolean.TRUE;
            ((MethodInvocation)invocation).setArguments(arguments);
         }

         // We don't retry the following invocations:
         // cancelDelivery(), cancelDeliveries(), cancelInflightMessages() - the deliveries will
         // already be cancelled after failover.

         if (methodName.equals("cancelDelivery") ||
            methodName.equals("cancelDeliveries"))
         {
            log.trace(this + " NOT resuming " + methodName + "(), let it wither and die");
            
            return null;
         }
         else
         {            
            log.trace(this + " resuming " + methodName + "()");
            
            return invocation.invokeNext();
         }
      } 
      catch (Throwable e)
      {
         // not failover-triggering, rethrow
         if (trace) { log.trace(this + " caught not failover-triggering throwable, rethrowing " + e); }
         throw e;
      }
      finally
      {
         if (!left)
         {
            valve.leave();
         }
      }
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "FailoverValveInterceptor." + (delegate == null ? "UNITIALIZED" : delegate.toString());
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}

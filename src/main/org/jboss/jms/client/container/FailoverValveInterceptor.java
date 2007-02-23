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
import org.jboss.jms.client.FailoverValve;
import org.jboss.jms.client.FailureDetector;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.util.MessagingNetworkFailureException;
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
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class FailoverValveInterceptor implements Interceptor, FailureDetector
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(FailoverValveInterceptor.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private DelegateSupport delegate;

   // We need to cache connectionState here, as FailureCommandCenter instance could be null for
   // non-clustered connections
   private ConnectionState connectionState;
   private FailoverCommandCenter fcc;
   private FailoverValve valve;

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

         // maintain a reference to the FailoverCommandCenter instance.
         fcc = connectionState.getFailoverCommandCenter();
         
         if (fcc != null)
         {
            valve = fcc.getValve();
         }
      }

      // non clustered, send the invocation forward
      if (fcc == null)
      {
         return invocation.invokeNext();
      }

      JMSRemotingConnection remotingConnection = null;
      String methodName = ((MethodInvocation)invocation).getMethod().getName();

      // consumer.receive should be ignored as it doesn't perform any server IO:
      // http://jira.jboss.org/jira/browse/JBMESSAGING-790
      if (invocation.getTargetObject() instanceof ClientConsumerDelegate &&
         methodName.equals("receive"))
      {
         return invocation.invokeNext();
      }

      try
      {
         valve.enter();

         // it's important to retrieve the remotingConnection while inside the Valve
         remotingConnection = fcc.getRemotingConnection();
         return invocation.invokeNext();
      }
      catch (MessagingNetworkFailureException e)
      {
         log.debug(this + " detected network failure, putting " + methodName +
         "() on hold until failover completes");
      
         fcc.failureDetected(e, this, remotingConnection);
         
         log.debug(this + " resuming " + methodName + "()");
      
         Object target = invocation.getTargetObject();
         
         // Set retry flag as true on send & sendTransaction
         // more details at http://jira.jboss.org/jira/browse/JBMESSAGING-809
      
         if (methodName.equals("send") &&
             target instanceof ClientSessionDelegate)
         {
            log.debug("#### Capturing send invocation.. setting check to true");
            Object[] arguments = ((MethodInvocation)invocation).getArguments();
            arguments[1] = Boolean.TRUE;
            ((MethodInvocation)invocation).setArguments(arguments);
         }
         else
         if (methodName.equals("sendTransaction") &&
             target instanceof ClientConnectionDelegate)
         {
            log.debug("#### Capturing sendTransaction invocation.. setting check to true");
            Object[] arguments = ((MethodInvocation)invocation).getArguments();
            arguments[1] = Boolean.TRUE;
            ((MethodInvocation)invocation).setArguments(arguments);
         }

         return invocation.invokeNext();
      } 
      catch (Throwable e)
      {
         // not failover-triggering, rethrow
         throw e;
      }
      finally
      {
         valve.leave();
      }
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "FailoverValve." + (delegate == null ? "UNITIALIZED" : delegate.toString());
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}

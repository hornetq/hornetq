/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.FailoverCommandCenter;
import org.jboss.jms.client.FailoverValve;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.remoting.CannotConnectException;

import java.io.IOException;

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
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class FailoverValveInterceptor implements Interceptor
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private DelegateSupport delegate;
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
      // maintain a reference to the FailoverCommandCenter instance.

      if (fcc == null)
      {
         delegate = (DelegateSupport)invocation.getTargetObject();

         HierarchicalState hs = delegate.getState();
         while (hs != null && !(hs instanceof ConnectionState))
         {
            hs = hs.getParent();
         }

         fcc = ((ConnectionState)hs).getFailoverCommandCenter();
         valve = fcc.getValve();
      }

      JMSRemotingConnection remotingConnection = null;

      try
      {
         valve.enter();

         // it's important to retrieve the remotingConnection while inside the Valve, as there's
         // guaranteed that no failover has happened yet
         // guarantee that no failover has happened yet
         remotingConnection = fcc.getRemotingConnection();
         return invocation.invokeNext();
      }
      catch (CannotConnectException e)
      {
         fcc.failureDetected(e, remotingConnection);
         return invocation.invokeNext();
      }
      catch (IOException e)
      {
         fcc.failureDetected(e, remotingConnection);
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

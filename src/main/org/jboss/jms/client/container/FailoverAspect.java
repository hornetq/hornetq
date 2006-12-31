/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.logging.Logger;

/**
 * PER_VM Aspect.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class FailoverAspect
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(FailoverAspect.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   public Object handlePerformFailover(Invocation invocation) throws Throwable
   {
      Object target = invocation.getTargetObject();

      if(target instanceof ClientConnectionDelegate)
      {
         performConnectionFailover((ClientConnectionDelegate)target);
      }

      return null;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void performConnectionFailover(ClientConnectionDelegate delegate) throws Exception
   {
      ConnectionState state = (ConnectionState)delegate.getState();

      CreateConnectionResult res = null;
      boolean failoverSuccessful = false;

      try
      {
         // block any other invocations ariving while we're doing failover, on this delegate and
         // recursively down the hierarchy

         // WARNING - this may block if there are active invocations through valves!
         delegate.closeValve();

         log.debug("starting client-side failover");

         // generate a FAILOVER_STARTED event. The event must be broadcasted AFTER valve closure,
         // to insure the client-side stack is in a deterministic state
         state.broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_STARTED, delegate));

         int failedNodeID = state.getServerID();
         ConnectionFactoryDelegate clusteredDelegate =
            state.getClusteredConnectionFactoryDelegate();

         // re-try creating the connection
         res = clusteredDelegate.
            createConnectionDelegate(state.getUsername(), state.getPassword(), failedNodeID);

         if (res == null)
         {
            // No failover attempt was detected on the server side; this might happen if the
            // client side network fails temporarily so the client connection breaks but the
            // server cluster is still up and running - in this case we don't perform failover.
            failoverSuccessful = false;
         }
         else
         {
            // recursively synchronize state
            ClientConnectionDelegate newDelegate = (ClientConnectionDelegate)res.getDelegate();
            delegate.synchronizeWith(newDelegate);
            failoverSuccessful = true;
         }
      }
      finally
      {
         if (failoverSuccessful)
         {
            state.broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_COMPLETED,
                                                           res.getDelegate()));
         }
         else
         {
            state.broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_FAILED,
                                                           delegate));
         }

         // failover done, open valves
         delegate.openValve();
      }
   }

   // Inner classes --------------------------------------------------------------------------------
}

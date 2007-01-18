/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.aop;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.endpoint.advised.ConnectionAdvised;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.jmx.rmi.RMITestServer;

/**
 * Used to force a "poisoned" server to do all sorts of bad things. Used for testing.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class PoisonInterceptor implements Interceptor
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PoisonInterceptor.class);
   
   public static final int TYPE_CREATE_SESSION = 0;
   
   public static final int TYPE_2PC_COMMIT = 1;
   

   // Static ---------------------------------------------------------------------------------------
   
   private static int type;
   
   public static void setType(int type)
   {
      PoisonInterceptor.type = type;
   }

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Interceptor implementation -------------------------------------------------------------------

   public String getName()
   {
      return "PoisonInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();

      if ("createSessionDelegate".equals(methodName) && type == TYPE_CREATE_SESSION)
      {
         // Used by the failover tests to kill server in the middle of an invocation.

         log.info("##### Crashing on createSessionDelegate!!");
         
         crash(invocation.getTargetObject());
      }
      else if ("sendTransaction".equals(methodName))
      {
         TransactionRequest request = (TransactionRequest)mi.getArguments()[0];
         
         if (request.getRequestType() == TransactionRequest.TWO_PHASE_COMMIT_REQUEST
             && type == TYPE_2PC_COMMIT)
         {
            //Crash on 2pc commit - used in message bridge tests
            
            log.info("##### Crashing on 2PC commit!!");
            
            crash(invocation.getTargetObject());            
         }
      }

      return invocation.invokeNext();
   }
  

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void crash(Object target) throws Exception
   {
      ConnectionAdvised advised = (ConnectionAdvised)target;
      
      ServerConnectionEndpoint endpoint = (ServerConnectionEndpoint)advised.getEndpoint();
      
      int serverId = endpoint.getServerPeer().getServerPeerID();
            
      //First unregister from the RMI registry
      Registry registry = LocateRegistry.getRegistry(RMITestServer.DEFAULT_REGISTRY_PORT);

      String name = RMITestServer.RMI_SERVER_PREFIX + serverId;
      registry.unbind(name);
      log.info("unregistered " + name + " from registry");

      name = RMITestServer.NAMING_SERVER_PREFIX + serverId;
      registry.unbind(name);
      log.info("unregistered " + name + " from registry");
      
      log.info("#####"); 
      log.info("#####");
      log.info("##### Halting the server!");
      log.info("#####");
      log.info("#####");

      Runtime.getRuntime().halt(1);
   }

   // Inner classes --------------------------------------------------------------------------------

}

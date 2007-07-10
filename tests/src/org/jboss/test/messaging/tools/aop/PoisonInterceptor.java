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
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.endpoint.advised.ConnectionAdvised;
import org.jboss.jms.server.endpoint.advised.ConnectionFactoryAdvised;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.jmx.rmi.RMITestServer;

/**
 * Used to force a "poisoned" server to do all sorts of bad things. Used for testing.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class PoisonInterceptor implements Interceptor
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PoisonInterceptor.class);
   
   public static final int NULL = -1;

   public static final int TYPE_CREATE_SESSION = 0;
   
   public static final int TYPE_2PC_COMMIT = 1;

   public static final int FAIL_AFTER_ACKNOWLEDGE_DELIVERY = 2;

   public static final int FAIL_BEFORE_ACKNOWLEDGE_DELIVERY = 3;

   public static final int FAIL_AFTER_SEND = 4;

   public static final int FAIL_BEFORE_SEND = 5;

   public static final int FAIL_SYNCHRONIZED_SEND_RECEIVE = 6;

   public static final int FAIL_AFTER_SENDTRANSACTION = 7;
   
   public static final int LONG_SEND = 8;

   public static final int CF_CREATE_CONNECTION= 9;

   public static final int CF_GET_CLIENT_AOP_STACK = 10;
   // Static ---------------------------------------------------------------------------------------
   
   private static int type = NULL;

   private static Object sync = new Object();
   
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

      if (type==NULL)
      {
         return invocation.invokeNext();
      }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();
      Object target = mi.getTargetObject();

      log.info("Invoke target=" + target.getClass().getName() + " method = " + methodName);

      if (target instanceof ConnectionAdvised && "createSessionDelegate".equals(methodName)
             && type == TYPE_CREATE_SESSION)
      {
         // Used by the failover tests to kill server in the middle of an invocation.

         log.info("##### Crashing on createSessionDelegate!!");
         
         crash(target);
      }
      else if (target instanceof ConnectionAdvised && "sendTransaction".equals(methodName))
      {
         TransactionRequest request = (TransactionRequest)mi.getArguments()[0];
         
         if (request.getRequestType() == TransactionRequest.TWO_PHASE_COMMIT_REQUEST
             && type == TYPE_2PC_COMMIT)
         {
            //Crash before 2pc commit (after prepare)- used in message bridge tests
            
            log.info("##### Crashing on 2PC commit!!");
            
            crash(target);
         }
         else if (request.getRequestType() == TransactionRequest.ONE_PHASE_COMMIT_REQUEST &&
             type == FAIL_AFTER_SENDTRANSACTION)
         {
            invocation.invokeNext();
            log.info("#### Crash after sendTransaction");
            crash(target);
         }
      }
      else if (target instanceof SessionAdvised && "acknowledgeDelivery".equals(methodName)
                 && type == FAIL_AFTER_ACKNOWLEDGE_DELIVERY)
      {
         invocation.invokeNext();

         log.info("##### Crashing after acknowledgeDelivery call!!!");

         // simulating failure right after invocation (before message is transmitted to client)
         crash(target);
      }
      else if (target instanceof SessionAdvised && "acknowledgeDelivery".equals(methodName)
                 && type == FAIL_BEFORE_ACKNOWLEDGE_DELIVERY)
      {

         log.info("##### Crashing before acknowledgeDelivery call!!!");

         crash(target);
      }
      else if (target instanceof SessionAdvised && "send".equals(methodName)
                 && type == FAIL_AFTER_SEND)
      {
         invocation.invokeNext();

         log.info("##### Crashing after send!!!");

         // On this case I really want to screw things up! I want the client to receive the message
         // not only after the send was executed.

         Thread.sleep(5000);

         crash(target);
      }
      else if (target instanceof SessionAdvised && "send".equals(methodName)
                 && type == FAIL_BEFORE_SEND)
      {
         log.info("##### Crashing before send!!!", new Exception());

         crash(target);
      }
      else if (type == FAIL_SYNCHRONIZED_SEND_RECEIVE)
      {
         if (target instanceof SessionAdvised && "send".equals(methodName))
         {
            invocation.invokeNext();
            synchronized (sync)
            {
               log.info("#### Will wait till an acknowledge comes to fail at the same time");
               sync.wait();
            }
            crash(target);
         }
         else if (target instanceof SessionAdvised && "acknowledgeDelivery".equals(methodName))
         {
            invocation.invokeNext();
            log.info("#### Notifying sender thread to crash the server, as ack was completed");
            synchronized (sync)
            {
               sync.notifyAll();
            }
            // lets sleep until the server is killed
            log.info("Waiting the synchronized send to kill this invocation.");
            Thread.sleep(60000);
         }
      }
      else if (type == LONG_SEND)
      {
         if ("send".equals(methodName))
         {
            //Pause for 2 mins before processing send
            log.info("Sleeping for 2 minutes before sending....");
            Thread.sleep(120000);
            
            invocation.invokeNext();
         }
      }
      else if (target instanceof ConnectionFactoryAdvised &&
               (type == CF_GET_CLIENT_AOP_STACK && "getClientAOPStack".equals(methodName))
               || (type == CF_CREATE_CONNECTION && "createConnectionDelegate".equals(methodName)))
      {
         crash(target);
      }

      return invocation.invokeNext();
   }
  

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private ServerPeer getServerPeer(Object obj) throws Exception
   {
      if (obj instanceof ConnectionAdvised)
      {
         ConnectionAdvised adv = (ConnectionAdvised) obj;
         ServerConnectionEndpoint endpoint = (ServerConnectionEndpoint )adv.getEndpoint();
         return endpoint.getServerPeer();
      }
      else if (obj instanceof SessionAdvised)
      {
         SessionAdvised adv = (SessionAdvised) obj;
         ServerSessionEndpoint endpoint = (ServerSessionEndpoint)adv.getEndpoint();
         return endpoint.getConnectionEndpoint().getServerPeer();
      }
      else if (obj instanceof ConnectionFactoryAdvised)
      {
         ConnectionFactoryAdvised adv = (ConnectionFactoryAdvised) obj;
         ServerConnectionFactoryEndpoint endpoint = (ServerConnectionFactoryEndpoint)adv.getEndpoint();
         return endpoint.getServerPeer();
      }
      else
      {
         throw new IllegalStateException("PoisonInterceptor doesn't support " +
            obj.getClass().getName() +
            " yet! You will have to implement getServerPeer for this class");
      }
   }

   private void crash(Object target) throws Exception
   {
      try
      {
         int serverId = getServerPeer(target).getServerPeerID();

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
      }
      finally
      {
         // this finally is for the the case where server0 was killed and unbind throws an exception
         // It shouldn't happen in our regular testsuite but it could happen on eventual
         // temporary tests not meant to commit.
         //
         // For example I needed to kill server0 to test AOPLoader while I couldn't commit the test
         Runtime.getRuntime().halt(1);
      }
   }

   // Inner classes --------------------------------------------------------------------------------

}

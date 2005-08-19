/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools;

import java.util.Hashtable;

import org.jboss.messaging.tools.jmx.ServiceContainer;
import org.jboss.messaging.tools.jmx.RemotingJMXWrapper;
import org.jboss.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.messaging.tools.jndi.RemoteInitialContextFactory;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jmx.adaptor.rmi.RMIAdaptor;
import org.jboss.remoting.transport.Connector;

import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.transaction.TransactionManager;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerManagement
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   private static ServiceContainer sc;
   private static ServerPeer serverPeer;
   private static boolean isRemote;
   private static RMIAdaptor rmiAdaptor;
   
   public static void setRemote(boolean remote)
   {
      isRemote = remote;
   }

   public synchronized static void startInVMServer() throws Exception
   {
      if (!isRemote) startInVMServer("transaction,remoting", null);
   }

   public synchronized static void startInVMServer(TransactionManager tm) throws Exception
   {
      if (!isRemote) startInVMServer("transaction,remoting", tm);
   }

   public synchronized static void startInVMServer(String config) throws Exception
   {
      if (!isRemote) startInVMServer(config, null);
   }

   /**
    * @param tm - specifies a specific TransactionManager instance to bind into the mbeanServer.
    *        If null, the default JBoss TransactionManager implementation will be used.
    */
   public synchronized static void startInVMServer(String config, TransactionManager tm)
         throws Exception
   {
      if (sc != null)
      {
         throw new Exception("server already started!");
      }
      sc = new ServiceContainer(config, tm);
      sc.start();
      serverPeer = new ServerPeer("ServerPeer0");
      serverPeer.start();
   }



   public synchronized static void stopInVMServer() throws Exception
   {
      if (sc == null)
      {
         return;
      }
      serverPeer.stop();
      serverPeer = null;
      sc.stop();
      sc = null;
   }

   public static ServerPeer getServerPeer() throws Exception
   {
      return serverPeer;
   }

   public static Connector getConnector() throws Exception
   {
      RemotingJMXWrapper remoting =
            (RemotingJMXWrapper)sc.getService(ServiceContainer.REMOTING_OBJECT_NAME);
      return remoting.getConnector();
   }

   public static void deployTopic(String name) throws Exception
   {
      deployTopic(name, null);
   }
   
   public static void deployTopic(String name, String jndiName) throws Exception
   {
      insureStarted();
      if (!isRemote)
      {
         
         serverPeer.getDestinationManager().createTopic(name, jndiName);
      }
      else
      {
         ObjectName on = new ObjectName("jboss.messaging:service=DestinationManager");
         
         rmiAdaptor.invoke(on, "createTopic",
               new Object[] {name, jndiName}, new String[] { "java.lang.String" ,
                                                             "java.lang.String"});
      }
   }

   public static void undeployTopic(String name) throws Exception
   {
      insureStarted();
      if (!isRemote)
      {
         
         serverPeer.getDestinationManager().destroyTopic(name);
      }
      else
      {
         ObjectName on = new ObjectName("jboss.messaging:service=DestinationManager");
         
         rmiAdaptor.invoke(on, "destroyTopic",
               new Object[] {name}, new String[] { "java.lang.String" });
      }
   }

   public static void deployQueue(String name) throws Exception
   {
      deployQueue(name, null);
   }
   
   public static void deployQueue(String name, String jndiName) throws Exception
   {
      insureStarted();
      if (!isRemote)
      {
    
         serverPeer.getDestinationManager().createQueue(name, jndiName);
      }
      else
      {
         ObjectName on = new ObjectName("jboss.messaging:service=DestinationManager");
         
         rmiAdaptor.invoke(on, "createQueue",
               new Object[] {name, jndiName}, new String[] { "java.lang.String" , "java.lang.String"});
      }
   }

   public static void undeployQueue(String name) throws Exception
   {
      insureStarted();
      if (!isRemote)
      {
       
         serverPeer.getDestinationManager().destroyQueue(name);
      }
      else
      {
         ObjectName on = new ObjectName("jboss.messaging:service=DestinationManager");
         
         rmiAdaptor.invoke(on, "destroyQueue",
               new Object[] {name}, new String[] { "java.lang.String" });
      }
   }

   public static Hashtable getJNDIEnvironment()
   {
      if (isRemote)
      {
         return RemoteInitialContextFactory.getJNDIEnvironment();
      }
      else
      {
         return InVMInitialContextFactory.getJNDIEnvironment();
      }
   }
   
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private static void insureStarted() throws Exception
   {
      if (isRemote)
      {
         if (rmiAdaptor == null)
         {
            setupRemoteJMX();
         }
      }
      else
      {
         if (sc == null)
         {
            throw new Exception("The server has not been started!");
         }
      }
   }
   
   private static void setupRemoteJMX() throws Exception
   {
      InitialContext ic = new InitialContext(RemoteInitialContextFactory.getJNDIEnvironment());
      
      rmiAdaptor = (RMIAdaptor) ic.lookup("jmx/invoker/RMIAdaptor");
      
     
      
   }

   // Inner classes -------------------------------------------------
}

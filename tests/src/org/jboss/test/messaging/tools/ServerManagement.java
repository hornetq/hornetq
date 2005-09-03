/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools;

import java.io.StringReader;
import java.util.Hashtable;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jmx.adaptor.rmi.RMIAdaptor;
import org.jboss.logging.Logger;
import org.jboss.remoting.transport.Connector;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.tools.jmx.MockJBossSecurityManager;
import org.jboss.test.messaging.tools.jmx.RemotingJMXWrapper;
import org.jboss.test.messaging.tools.jndi.RemoteInitialContextFactory;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.transaction.TransactionManager;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Collection of static methods to use to start/stop and interact with the in memory JMS server. 
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
   
   private static Logger log = Logger.getLogger(ServerManagement.class);
   
   private static void initRemote()
   {
      String remoteVal = System.getProperty("remote");
      if ("true".equals(remoteVal))
      {
         isRemote = true;
      }
      if (isRemote)
      {
         log.info("*** Tests are running against remote instance");
      }
      else
      {
         log.info("*** Tests are running in INVM container");
      }      
   }
   
   public static synchronized void init() throws Exception
   {
      initRemote();
      if (!isRemote)
      {
         startInVMServer();
      }
   }
   
   public static void init(TransactionManager tm) throws Exception
   {
      initRemote();
      if (!isRemote)
      {
         startInVMServer(tm);
      }
   }
   
   public static void init(String config) throws Exception
   {
      initRemote();
      if (!isRemote)
      {
         startInVMServer(config);
      }
   }
   
   public static boolean isRemote()
   {
      return isRemote;
   }

   private static void startInVMServer() throws Exception
   {
      startInVMServer("transaction, remoting, aop, security", null);
   }

   private static void startInVMServer(TransactionManager tm) throws Exception
   {
      startInVMServer("transaction, remoting, aop, security", tm);
   }

   private synchronized static void startInVMServer(String config) throws Exception
   {
      startInVMServer(config, null);
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
      serverPeer.setSecurityDomain(MockJBossSecurityManager.TEST_SECURITY_DOMAIN);
      final String defaultSecurityConfig = 
         "<security><role name=\"guest\" read=\"true\" write=\"true\" create=\"true\"/></security>";
      serverPeer.setDefaultSecurityConfig(toElement(defaultSecurityConfig));
      serverPeer.start();
   }
   
   private static Element toElement(String s)
      throws Exception
   {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder parser = factory.newDocumentBuilder();
      Document doc = parser.parse(new InputSource(new StringReader(s)));
      return doc.getDocumentElement();
   }



   public synchronized static void deInit() throws Exception
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
   
   public static void setSecurityConfig(String destName, Element config) throws Exception
   {
      if (isRemote)
      {
         ObjectName on = new ObjectName("jboss.messaging:service=ServerPeer");
         
         rmiAdaptor.invoke(on, "setSecurityConfig",
               new Object[] {destName, config}, new String[] { "java.lang.String" ,
                                                             "org.w3c.dom.Element"});
      }
      else
      {
         ServerManagement.getServerPeer().setSecurityConfig(destName, config);
      }
   }
   
   public static void setDefaultSecurityConfig(Element config) throws Exception
   {
      if (isRemote)
      {
         ObjectName on = new ObjectName("jboss.messaging:service=ServerPeer");
         
         rmiAdaptor.invoke(on, "setDefaultSecurityConfig",
               new Object[] {config}, new String[] { "org.w3c.dom.Element"});
      }
      else
      {
         ServerManagement.getServerPeer().setDefaultSecurityConfig(config);
      }
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

/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tools;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.aop.AspectXmlLoader;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;

import javax.naming.InitialContext;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import java.net.URL;

/**
 * A place-holder for the micro-container. Used to bootstrap a server instance, until proper
 * integration with the micro-container. Run it with bin/runserver.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerWrapper
{
   // Constants -----------------------------------------------------

   protected static final Logger log = Logger.getLogger(ServerWrapper.class);

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      log.info("Starting the server ...");
      new ServerWrapper().start();
   }

   // Attributes ----------------------------------------------------

   private ServerPeer serverPeer;
   private InvokerLocator locator = new InvokerLocator("socket://localhost:9890");
   private String serverPeerID = "ServerPeer0";

   // Constructors --------------------------------------------------

   public ServerWrapper() throws Exception
   {
      loadAspects();
      setupJNDI();
      initializeRemoting(locator);
      serverPeer = new ServerPeer(serverPeerID, locator);
      start();
   }

   // Public --------------------------------------------------------

   public void start() throws Exception
   {
      serverPeer.start();
      log.info("Server started.");
   }

   public void stop() throws Exception
   {
      serverPeer.stop();
   }

   public void deployQueue(String name) throws Exception
   {
      InitialContext ic = new InitialContext();
      Context c = (Context)((Context)ic.lookup("messaging")).lookup("queues");
      c.rebind(name, new JBossQueue(name));
   }

   public void deployTopic(String name) throws Exception
   {
      InitialContext ic = new InitialContext();
      Context c = (Context)((Context)ic.lookup("messaging")).lookup("topics");
      c.rebind(name, new JBossTopic(name));
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void loadAspects() throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("jms-aop.xml");
      AspectXmlLoader.deployXML(url);
   }


   private void setupJNDI() throws Exception
   {
      InitialContext ic = new InitialContext();
      Context c = null;
      String path = "/";
      String name = "messaging";

      try
      {
         c = (Context)ic.lookup(name);
      }
      catch(NameNotFoundException e)
      {
         c = ic.createSubcontext(name);
         log.info("Created context " + path + name);
      }

      path += name + "/";
      String[] names = {"topics", "queues" };

      for (int i = 0; i < names.length; i++)
      {
         try
         {
            c.lookup(names[i]);
         }
         catch(NameNotFoundException e)
         {
            c.createSubcontext(names[i]);
            log.info("Created context " + path + names[i]);
         }
      }
   }

   private void initializeRemoting(InvokerLocator locator) throws Exception
   {
      Connector connector = new Connector();
      connector.setInvokerLocator(locator.getLocatorURI());
      connector.start();

      // also add the JMS subsystem
      connector.addInvocationHandler("JMS", new JMSServerInvocationHandler());
   }

   // Inner classes -------------------------------------------------
}

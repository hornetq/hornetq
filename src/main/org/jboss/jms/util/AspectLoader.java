/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.util;

import org.jboss.logging.Logger;

import javax.management.ObjectName;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.net.URL;
import java.util.ArrayList;

/**
 * Service that helps overcome some of the problems related to aspect scoping and loading in
 * JBoss 4. The service explicitely loads jms-aop.xml using the scoped Aspect Deployer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class AspectLoader
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(AspectLoader.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private ObjectName mainDeployer;

   // Constructors --------------------------------------------------

   // JMX attribute accessors ---------------------------------------

   public void setMainDeployer(ObjectName on)
   {
      mainDeployer = on;
   }

   // JMX operations ------------------------------------------------

   public void create()
   {
      // noop
   }

   public void start() throws Exception
   {
      // since I have a dependency on my scoped Aspect Deployer, the scoped Aspect Deployer will
      // be installed and fully operational at this time

      String aspectFile = "jms-aop.xml";
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      URL url = cl.getResource(aspectFile);

      if (url == null)
      {
         throw new Exception("Aspect file " + aspectFile + " not found in the classpath!");
      }

      log.debug("aspects file URL: " + url);

      ArrayList servers = MBeanServerFactory.findMBeanServer(null);
      MBeanServer server = (MBeanServer)servers.iterator().next();

      if (server == null)
      {
         throw new Exception("Cannot findt the JBoss MBeanServer");
      }

      server.invoke(mainDeployer, "deploy", new Object[] {url}, new String[] { "java.net.URL"} );

      log.debug("Aspects deployed");

      // TODO I can undeploy my scoped Aspect Deployer here and save some later trouble, when folks want to deploy their own aspects.
   }

   public void stop()
   {
      // noop
   }

   public void destroy()
   {
      // noop
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}


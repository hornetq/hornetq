/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting.util;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.remoting.InvocationRequest;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.MBeanRegistration;
import java.lang.reflect.Constructor;

/**
 * A standard MBean service to be used when testing remoting.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemotingTestSubsystemService
   implements MBeanRegistration, RemotingTestSubsystemServiceMBean
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingTestSubsystemService.class);

   public static final String SUBSYSTEM_LABEL = "TEST_SUBSYSTEM";

   // Static ---------------------------------------------------------------------------------------

   public static ObjectName deployService() throws Exception
   {
      return deployService("org.jboss.test.thirdparty.remoting.util.RemotingTestSubsystem");
   }

   public static ObjectName deployService(String subsystemClassName) throws Exception
   {
      String testSubsystemConfig =
         "<mbean code=\"org.jboss.test.thirdparty.remoting.util.RemotingTestSubsystemService\"\n" +
            " name=\"test:service=RemotingTestSubsystem\">\n" +
            "<attribute name=\"SubsystemClassName\">" + subsystemClassName + "</attribute>" +
         "</mbean>";

      ObjectName on = ServerManagement.deploy(testSubsystemConfig);
      ServerManagement.invoke(on, "start", new Object[0], new String[0]);

      return on;
   }

   public static void undeployService(ObjectName on) throws Exception
   {
      ServerManagement.invoke(on, "stop", new Object[0], new String[0]);
      ServerManagement.undeploy(on);
   }

   public static InvocationRequest getNextInvocationFromServer(ObjectName on, long timeout)
      throws Exception
   {
      return (InvocationRequest)ServerManagement.
         invoke(on, "nextInvocation",
                new Object[] { new Long(timeout) },
                new String[] { "java.lang.Long" });
   }

   public static boolean isFailed(ObjectName on)
      throws Exception
   {
      return ((Boolean)ServerManagement.
         invoke(on, "isFailed", null, null)).booleanValue();
   }

   // Attributes -----------------------------------------------------------------------------------

   private MBeanServer mbeanServer;
   private ObjectName myObjectName;

   private String subsystemClassName;
   private TestableSubsystem delegate;

   // Constructors ---------------------------------------------------------------------------------

   // MBeanRegistration implementation -------------------------------------------------------------

   public ObjectName preRegister(MBeanServer mbeanServer, ObjectName objectName) throws Exception
   {
      this.mbeanServer = mbeanServer;
      this.myObjectName = objectName;
      return objectName;
   }

   public void postRegister(Boolean b)
   {
      // noop
   }

   public void preDeregister() throws Exception
   {
      // noop
   }

   public void postDeregister()
   {
      // noop
   }

   // RemotingTestSubsystemServiceMBean implementation ---------------------------------------------

   public String getSubsystemClassName()
   {
      return subsystemClassName;
   }

   public void setSubsystemClassName(String className)
   {
      this.subsystemClassName = className;
   }

   public void start() throws Exception
   {
      Class c = Class.forName(subsystemClassName);
      Constructor cons = c.getConstructor(new Class[0]);

      delegate = (TestableSubsystem)cons.newInstance(new Object[0]);

      // register to the remoting connector

      mbeanServer.invoke(ServiceContainer.REMOTING_OBJECT_NAME,
                         "addInvocationHandler",
                         new Object[] {SUBSYSTEM_LABEL, delegate},
                         new String[] {"java.lang.String",
                            "org.jboss.remoting.ServerInvocationHandler"});

      log.debug(myObjectName + " started");
   }

   public void stop()
   {
      try
      {
         // unregister from the remoting connector

         mbeanServer.invoke(ServiceContainer.REMOTING_OBJECT_NAME,
                            "removeInvocationHandler",
                            new Object[] {SUBSYSTEM_LABEL},
                            new String[] {"java.lang.String"});
      }
      catch(Exception e)
      {
         log.error("Cannot deinstall remoting subsystem", e);
      }

      delegate = null;

      log.debug(myObjectName + " stopped");
   }

   public InvocationRequest nextInvocation(Long timeout) throws Exception
   {
      if (delegate == null)
      {
         return null;
      }

      return delegate.getNextInvocation(timeout.longValue());

   }

   public boolean isFailed() throws Exception
   {
      return delegate.isFailed();
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}

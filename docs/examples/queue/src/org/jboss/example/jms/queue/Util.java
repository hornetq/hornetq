/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.queue;

import javax.naming.InitialContext;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Util
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static void deployQueue(String jndiName) throws Exception
   {
      MBeanServerConnection mBeanServer = lookupMBeanServerProxy();

      ObjectName destinationManager = new ObjectName("jboss.messaging:service=DestinationManager");

      String queueName = jndiName.substring(jndiName.lastIndexOf('/') + 1);

      mBeanServer.invoke(destinationManager, "createQueue",
                         new Object[] {queueName, jndiName},
                         new String[] {"java.lang.String", "java.lang.String"});

      System.out.println("Queue " + jndiName + " deployed");
   }

   public static void undeployQueue(String jndiName) throws Exception
   {
      MBeanServerConnection mBeanServer = lookupMBeanServerProxy();

      ObjectName destinationManager = new ObjectName("jboss.messaging:service=DestinationManager");

      String queueName = jndiName.substring(jndiName.lastIndexOf('/') + 1);

      mBeanServer.invoke(destinationManager, "destroyQueue",
                         new Object[] {queueName},
                         new String[] {"java.lang.String"});

      System.out.println("Queue " + jndiName + " undeployed");
   }


   public static MBeanServerConnection lookupMBeanServerProxy() throws Exception
   {
      InitialContext ic = new InitialContext();
      MBeanServerConnection p = (MBeanServerConnection)ic.lookup("jmx/invoker/RMIAdaptor");
      ic.close();
      return p;
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.jmx;

import org.jboss.mx.server.MBeanServerImpl;

import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MBeanServerBuilder extends javax.management.MBeanServerBuilder
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public MBeanServerBuilder()
   {
   }

   // MBeanServerBuilder overrides ----------------------------------

   public MBeanServer newMBeanServer(String defaultDomain,
                                     MBeanServer outer,
                                     MBeanServerDelegate delegate)
   {
      return new MBeanServerImpl("jboss", outer, delegate);
   }

   public MBeanServerDelegate	newMBeanServerDelegate()
   {
      return new MBeanServerDelegate();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

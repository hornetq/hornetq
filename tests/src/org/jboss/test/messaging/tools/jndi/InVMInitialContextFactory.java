/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.jndi;

import org.jboss.logging.Logger;

import javax.naming.spi.InitialContextFactory;
import javax.naming.NamingException;
import javax.naming.Context;
import java.util.Hashtable;

/**
 * An in-VM JNDI InitialContextFactory. Lightweight JNDI implementation used for testing.

 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class InVMInitialContextFactory implements InitialContextFactory
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(InVMInitialContextFactory.class);

   private static InVMContext initialContext;

   // Static --------------------------------------------------------

   /**
    * @return the JNDI environment to use to get this InitialContextFactory.
    */
   public static Hashtable getJNDIEnvironment()
   {
      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial",
              "org.jboss.messaging.tools.jndi.InVMInitialContextFactory");
      env.put("java.naming.provider.url", "");
      env.put("java.naming.factory.url.pkgs", "");
      return env;
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   public Context getInitialContext(Hashtable environment) throws NamingException
   {
      if (initialContext == null)
      {
         initialContext = new InVMContext();
         initialContext.bind("java:/", new InVMContext());
      }
      return initialContext;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

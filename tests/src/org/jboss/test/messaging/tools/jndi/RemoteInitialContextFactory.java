/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.test.messaging.tools.jndi;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.jboss.logging.Logger;

/**
 * An InitialContextFactory providing InitialContext to JNDI on a remote VM

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemoteInitialContextFactory implements InitialContextFactory
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemoteInitialContextFactory.class);

   private static InVMContext initialContext;

   // Static --------------------------------------------------------

   /**
    * @return the JNDI environment to use to get this InitialContextFactory.
    */
   public static Hashtable getJNDIEnvironment()
   {
      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
      env.put("java.naming.provider.url", "jnp://localhost:1099");
      env.put("java.naming.factory.url.pkg", "org.jboss.naming:org.jnp.interfaces");
      return env;
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   public Context getInitialContext(Hashtable environment) throws NamingException
   {
      return new InitialContext(environment);
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

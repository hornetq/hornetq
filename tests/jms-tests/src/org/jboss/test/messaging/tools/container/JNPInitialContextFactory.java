/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.jboss.test.messaging.tools.container;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

/**
 * An InitialContextFactory providing InitialContext to JNDI on a remote JBoss instance.

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2685 $</tt>
 *
 * $Id: JNPInitialContextFactory.java 2685 2007-05-15 07:56:12Z timfox $
 */
public class JNPInitialContextFactory implements InitialContextFactory
{
   // Constants -----------------------------------------------------

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

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
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2868 $</tt>
 *
 * $Id: RemoteInitialContextFactory.java 2868 2007-07-10 20:22:16Z timfox $
 */
public class RemoteInitialContextFactory implements InitialContextFactory
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemoteInitialContextFactory.class);

   // Static --------------------------------------------------------

   /**
    * @return the JNDI environment to use to get this InitialContextFactory.
    */
   public static Hashtable getJNDIEnvironment(int serverIndex)
   {
      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial",
              "org.jboss.test.messaging.tools.container.RemoteInitialContextFactory");
      env.put("java.naming.provider.url", "");
      env.put("java.naming.factory.url.pkgs", "");
      env.put(Constants.SERVER_INDEX_PROPERTY_NAME, Integer.toString(serverIndex));
      return env;
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   public Context getInitialContext(Hashtable environment) throws NamingException
   {
      String s = (String)environment.get(Constants.SERVER_INDEX_PROPERTY_NAME);
      
      if (s == null)
      {
         throw new IllegalArgumentException("Initial context environment must contain " +
                                            "entry for " + Constants.SERVER_INDEX_PROPERTY_NAME);
      }

      int remoteServerIndex = Integer.parseInt(s);

      try
      {
         return new RemoteContext(remoteServerIndex);
      }
      catch(Exception e)
      {
         log.error("Cannot get the remote context", e);
         throw new NamingException("Cannot get the remote context");
      }

   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

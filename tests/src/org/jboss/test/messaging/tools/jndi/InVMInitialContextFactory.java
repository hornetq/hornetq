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
package org.jboss.test.messaging.tools.jndi;

import javax.naming.spi.InitialContextFactory;
import javax.naming.NamingException;
import javax.naming.Context;
import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;

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

   // Static --------------------------------------------------------

   // List<InitialContext>
   private static List initialContexts;

   static
   {
      initialContexts = new ArrayList();
   }

   public static Hashtable getJNDIEnvironment()
   {
      return getJNDIEnvironment(0);
   }

   /**
    * @return the JNDI environment to use to get this InitialContextFactory.
    */
   public static Hashtable getJNDIEnvironment(int serverIndex)
   {
      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial",
              "org.jboss.messaging.tools.jndi.InVMInitialContextFactory");
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
      // try first in the environment passed as argument ...
      String s = (String)environment.get(Constants.SERVER_INDEX_PROPERTY_NAME);

      if (s == null)
      {
         // ... then in the global environment
         s = System.getProperty(Constants.SERVER_INDEX_PROPERTY_NAME);

         if (s == null)
         {
            throw new NamingException("Cannot figure out server index!");
         }
      }

      int serverIndex;

      try
      {
         serverIndex = Integer.parseInt(s);
      }
      catch(Exception e)
      {
         throw new NamingException("Failure parsing \"" +
                                   Constants.SERVER_INDEX_PROPERTY_NAME +"\". " +
                                   s + " is not an integer");
      }

      int size = initialContexts.size();

      // pad the list to the right size

      if (size <= serverIndex)
      {
         for(int i = 0; i < serverIndex - size + 1; i++)
         {
            initialContexts.add(null);
         }
      }

      InVMContext ic = (InVMContext)initialContexts.get(serverIndex);

      if (ic == null)
      {
         ic = new InVMContext();
         ic.bind("java:/", new InVMContext());
         initialContexts.set(serverIndex, ic);
      }

      return ic;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------   
}

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

import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.naming.spi.InitialContextFactoryBuilder;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision: 2868 $</tt>
 *
 * $Id: InVMInitialContextFactoryBuilder.java 2868 2007-07-10 20:22:16Z timfox $
 */
public class InVMInitialContextFactoryBuilder implements InitialContextFactoryBuilder
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(InVMInitialContextFactoryBuilder.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public InVMInitialContextFactoryBuilder()
   {
   }

   // InitialContextFactoryBuilder implementation --------------------------------------------------

   public InitialContextFactory createInitialContextFactory(Hashtable environment)
         throws NamingException
   {

      InitialContextFactory icf = null;

      if (environment != null)
      {
         String icfName = (String)environment.get("java.naming.factory.initial");

         if (icfName != null)
         {
            Class c = null;

            try
            {
               c = Class.forName(icfName);
            }
            catch(ClassNotFoundException e)
            {
               log.error("\"" + icfName + "\" cannot be loaded", e);
               throw new NamingException("\"" + icfName + "\" cannot be loaded");
            }

            try
            {
               icf = (InitialContextFactory)c.newInstance();
            }
            catch(InstantiationException e)
            {
               log.error(c.getName() + " cannot be instantiated", e);
               throw new NamingException(c.getName() + " cannot be instantiated");
            }
            catch(IllegalAccessException e)
            {
               log.error(c.getName() + " instantiation generated an IllegalAccessException", e);
               throw new NamingException(c.getName() +
                  " instantiation generated an IllegalAccessException");
            }
         }
      }

      if (icf == null)
      {
         icf = new InVMInitialContextFactory();
      }

      return icf;
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}

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
package org.jboss.test.messaging.tools.jmx.rmi;


import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class NamingDelegateImpl extends UnicastRemoteObject implements NamingDelegate
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContextAccess ica;

   // Constructors --------------------------------------------------

   public NamingDelegateImpl() throws Exception
   {
      super();
      ica = new InitialContextAccess();
   }

   // NamingDelegate implementation ---------------------------------

   public Object lookup(String name) throws Exception
   {
      return getInitialContext().lookup(name);
   }

   // Public --------------------------------------------------------

   public void reset()
   {
      ica.reset();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private InitialContext getInitialContext() throws NamingException
   {
      return ica.getInitialContext();
   }

   // Inner classes -------------------------------------------------

   private class InitialContextAccess
   {
      private InitialContext ic;

      InitialContext getInitialContext() throws NamingException
      {
         if (ic == null)
         {
            ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
         }
         return ic;
      }

      public void reset()
      {
         ic = null;
      }
   }
}

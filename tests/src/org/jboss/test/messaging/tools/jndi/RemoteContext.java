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

import java.rmi.Naming;
import java.util.Hashtable;
import javax.naming.*;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.test.messaging.tools.jmx.rmi.NamingDelegate;
import org.jboss.test.messaging.tools.jmx.rmi.RMITestServer;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemoteContext implements Context
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemoteContext.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private NamingDelegate namingDelegate;
   
   // Constructors --------------------------------------------------

   public RemoteContext(int remoteServerIndex) throws Exception
   {
      String n =
         "//localhost:" + RMITestServer.DEFAULT_REGISTRY_PORT + "/" +
          RMITestServer.NAMING_SERVER_PREFIX + remoteServerIndex;
      
      log.info("Using this url for rmi server lookup " + n);
      
      namingDelegate = (NamingDelegate)Naming.lookup(n);
   }

   // Context implementation ----------------------------------------

   public Object lookup(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object lookup(String name) throws NamingException
   {
      try
      {
         return namingDelegate.lookup(name);
      }
      catch(Exception e)
      {
         log.error("naming operation failed", e);
         throw new NamingException(e.getMessage());
      }
   }

   public void bind(Name name, Object obj) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void bind(String name, Object obj) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void rebind(Name name, Object obj) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void rebind(String name, Object obj) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void unbind(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void unbind(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void rename(Name oldName, Name newName) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void rename(String oldName, String newName) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NamingEnumeration list(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NamingEnumeration list(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NamingEnumeration listBindings(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NamingEnumeration listBindings(String contextName) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void destroySubcontext(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void destroySubcontext(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Context createSubcontext(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Context createSubcontext(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object lookupLink(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object lookupLink(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NameParser getNameParser(Name name) throws NamingException
   {
      return getNameParser(name.toString());
   }

   public NameParser getNameParser(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Name composeName(Name name, Name prefix) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public String composeName(String name, String prefix) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object addToEnvironment(String propName, Object propVal) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object removeFromEnvironment(String propName) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Hashtable getEnvironment() throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void close() throws NamingException
   {
   }

   public String getNameInNamespace() throws NamingException
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}


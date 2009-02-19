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
package org.jboss.test.jms;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.logging.Logger;
import org.objectweb.jtests.jms.admin.Admin;

/**
 * GenericAdmin.
 * 
 * @FIXME delegate to a JBoss defined admin class
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @version $Revision: 1.1 $
 */
public class GenericAdmin implements Admin
{
   public static final Logger log = Logger.getLogger(GenericAdmin.class);
   
   public static Admin delegate = new AbstractAdmin();

   public String getName()
   {
      String name = delegate.getName();
      log.debug("Using admin '" + name + "' delegate=" + delegate);
      return name;
   }
   
   public void start() throws Exception
   {
   }
   
   public void stop() throws Exception
   {
   }
   
   public Context createContext() throws NamingException
   {
      Context ctx = delegate.createContext();
      log.debug("Using initial context: " + ctx.getEnvironment());
      return ctx;
   }

   public void createConnectionFactory(String name)
   {
      log.debug("createConnectionFactory '" + name + "'");
      delegate.createConnectionFactory(name);
   }

   public void deleteConnectionFactory(String name)
   {
      log.debug("deleteConnectionFactory '" + name + "'");
      delegate.deleteConnectionFactory(name);
   }

   public void createQueue(String name)
   {
      log.debug("createQueue '" + name + "'");
      delegate.createQueue(name);
   }

   public void deleteQueue(String name)
   {
      log.debug("deleteQueue '" + name + "'");
      delegate.deleteQueue(name);
   }

   public void createQueueConnectionFactory(String name)
   {
      log.debug("createQueueConnectionFactory '" + name + "'");
      delegate.createQueueConnectionFactory(name);
   }

   public void deleteQueueConnectionFactory(String name)
   {
      log.debug("deleteQueueConnectionFactory '" + name + "'");
      delegate.deleteQueueConnectionFactory(name);
   }

   public void createTopic(String name)
   {
      log.debug("createTopic '" + name + "'");
      delegate.createTopic(name);
   }

   public void deleteTopic(String name)
   {
      log.debug("deleteTopic '" + name + "'");
      delegate.deleteTopic(name);
   }

   public void createTopicConnectionFactory(String name)
   {
      log.debug("createTopicConnectionFactory '" + name + "'");
      delegate.createTopicConnectionFactory(name);
   }

   public void deleteTopicConnectionFactory(String name)
   {
      log.debug("deleteTopicConnectionFactory '" + name + "'");
      delegate.deleteTopicConnectionFactory(name);
   }
   
   public void startServer() throws Exception
   {
      log.debug("startEmbeddedServer");
      delegate.startServer();
   }
   
   public void stopServer() throws Exception
   {
      log.debug("stopEmbeddedServer");
      delegate.stopServer();
   }
}

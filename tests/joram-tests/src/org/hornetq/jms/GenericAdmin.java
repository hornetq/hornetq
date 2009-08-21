/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms;

import javax.naming.Context;
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

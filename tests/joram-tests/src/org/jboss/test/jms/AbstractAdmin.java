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

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.util.NotImplementedException;
import org.objectweb.jtests.jms.admin.Admin;

/**
 * AbstractAdmin.
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @version $Revision: 1.1 $
 */
public class AbstractAdmin implements Admin
{
   public String getName()
   {
      return getClass().getName();
   }
   
   public void start()
   {
   }

   public void stop() throws Exception
   {
     
      
   }
   
   public InitialContext createContext() throws NamingException
   {
      return new InitialContext();
   }
   
   public void createConnectionFactory(String name)
   {
      throw new NotImplementedException("FIXME NYI createConnectionFactory");
   }

   public void deleteConnectionFactory(String name)
   {
      throw new NotImplementedException("FIXME NYI deleteConnectionFactory");
   }

   public void createQueue(String name)
   {
      throw new NotImplementedException("FIXME NYI createQueue");
   }

   public void deleteQueue(String name)
   {
      throw new NotImplementedException("FIXME NYI deleteQueue");
   }

   public void createQueueConnectionFactory(String name)
   {
      createConnectionFactory(name);
   }

   public void deleteQueueConnectionFactory(String name)
   {
      deleteConnectionFactory(name);
   }

   public void createTopic(String name)
   {
      throw new NotImplementedException("FIXME NYI createTopic");
   }

   public void deleteTopic(String name)
   {
      throw new NotImplementedException("FIXME NYI deleteTopic");
   }

   public void createTopicConnectionFactory(String name)
   {
      createConnectionFactory(name);
   }

   public void deleteTopicConnectionFactory(String name)
   {
      deleteConnectionFactory(name);
   }
   
   public void startServer()
   {
   }
   
   public void stopServer()
   {
   }
}

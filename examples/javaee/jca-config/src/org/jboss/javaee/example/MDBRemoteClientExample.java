/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.javaee.example;

import org.jboss.javaee.example.server2.StatelessSenderService;

import javax.jms.Connection;
import javax.naming.InitialContext;

/**
 * 
 * MDB Remote & JCA Configuration Example.
 * 
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class MDBRemoteClientExample
{
   public static void main(String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         //Step 2. Getting a reference to the Stateless Bean
         StatelessSenderService sender = (StatelessSenderService)initialContext.lookup("jca-config-example2/StatelessSender/remote");
         
         //Step 3. Calling a Stateless Session Bean. You will have more steps on the SessionBean
         sender.sendHello("Hello there MDB!");
         
         System.out.println("Step 3: Invoking the Stateless Bean");
         
         initialContext.close();
      }
      finally
      {
         //Step 11. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }
}

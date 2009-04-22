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

import javax.naming.InitialContext;

import org.jboss.javaee.example.server.XARecoveryExampleService;

/**
 * An example which invokes an EJB. The EJB will "pause" the server so that it
 * can be "crashed" to show how recovery works when the server is restarted.
 * 
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class XARecoverySenderExample
{
   public static void main(String[] args) throws Exception
   {
      InitialContext initialContext = null;
      try
      {
         // Step 1. Obtain an Initial Context
         initialContext = new InitialContext();

         // Step 2. Lookup the EJB
         XARecoveryExampleService service = (XARecoveryExampleService)initialContext.lookup("mdb-example/XARecoveryExampleBean/remote");

         // Step 3. Invoke the sendAndUpdate method
         service.send("This is a text message");
         System.out.println("invoked the EJB service");
      }
      finally
      {
         // Step 4. Be sure to close the resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }
}

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

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:csuconic@jboss.org">Clebert Suconic</a>
 */
public class HAJNDIExample
{
   public static void main(String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         
         
         //Step 1. Create an initial context to perform the JNDI lookup.
         //        This JNDI is performing auto-discovery of the servers, by using the default UDP properties.
         //        You will find more information at the JBoss Application Server Documentation:
         //        http://www.jboss.org/file-access/default/members/jbossas/freezone/docs/Clustering_Guide/5/html/clustering-jndi.html
         
         Hashtable jndiParameters = new Hashtable();
         jndiParameters.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         jndiParameters.put("java.naming.factory.url.pkgs=", "org.jboss.naming:org.jnp.interfaces");
         
         initialContext = new InitialContext(jndiParameters);

         for (int i = 0; i < 100; i++)
         {
            //Step 2. Perform a lookup on the Connection Factory
            ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
   
            //Step 3. Create a JMS Connection
            connection = cf.createConnection();
            connection.close();
            
            //Step 4. Kill any of the servers. The Lookups will still be performed ok as long as you keep at least one server alive.
            System.out.println("Connection " + i + " was created. If you kill any of the servers now, the lookup operation on Step 2 will still work fine");
            Thread.sleep(5000);
         }
         
         System.out.println("Done");

      }
      finally
      {
         //Step 5. Be sure to close our JMS resources!
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

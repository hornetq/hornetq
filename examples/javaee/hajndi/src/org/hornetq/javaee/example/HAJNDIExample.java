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
package org.hornetq.javaee.example;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;

/**
 * 
 * This example demonstrates the use of HA-JNDI to look-up JMS Connection Factories from JNDI.
 * 
 * For more information please see the readme.html file.
 * 
 * @author <a href="mailto:csuconic@jboss.org">Clebert Suconic</a>
 */
public class HAJNDIExample
{
   public static void main(final String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;

      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         // This JNDI is performing auto-discovery of the servers, by using the default UDP properties.
         // You will find more information at the JBoss Application Server Documentation:
         // http://www.jboss.org/file-access/default/members/jbossas/freezone/docs/Clustering_Guide/5/html/clustering-jndi.html

         Hashtable<String, String> jndiParameters = new Hashtable<String, String>();
         jndiParameters.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         jndiParameters.put("java.naming.factory.url.pkgs=", "org.jboss.naming:org.jnp.interfaces");

         initialContext = new InitialContext(jndiParameters);

         for (int i = 0; i < 100; i++)
         {
            // Step 2. Perform a lookup on the Connection Factory
            ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

            // Step 3. Create a JMS Connection
            connection = cf.createConnection();
            connection.close();

            // Step 4. Kill any of the servers. The Lookups will still be performed ok as long as you keep at least one
            // server alive.
            System.out.println("Connection " + i +
                               " was created. If you kill any of the servers now, the lookup operation on Step 2 will still work fine");
            Thread.sleep(5000);
         }

         System.out.println("Done");
      }
      finally
      {
         // Step 5. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection != null)
         {
            connection.close();
         }
      }
   }
}

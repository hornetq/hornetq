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

import java.util.ArrayList;
import java.util.List;

import javax.management.ObjectName;

/**
 * An MBeanServer and a configurable set of services (TransactionManager, Remoting, etc) available
 * for testing.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public class ServiceContainer
{
   // Constants ------------------------------------------------------------------------------------

   //private static final Logger log = Logger.getLogger(ServiceContainer.class);

   private static final String CONFIGURATION_FILE_NAME = "container.xml";

   public static final String DO_NOT_USE_MESSAGING_MARSHALLERS = "DO_NOT_USE_MESSAGING_MARSHALLERS";

   // Static ---------------------------------------------------------------------------------------

   public static ObjectName SERVICE_CONTROLLER_OBJECT_NAME;
   public static ObjectName CLASS_LOADER_OBJECT_NAME;
   public static ObjectName TRANSACTION_MANAGER_OBJECT_NAME;
   public static ObjectName CACHED_CONNECTION_MANAGER_OBJECT_NAME;

   public static ObjectName DEFAULTDS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME;
   public static ObjectName DEFAULTDS_MANAGED_CONNECTION_POOL_OBJECT_NAME;
   public static ObjectName DEFAULTDS_CONNECTION_MANAGER_OBJECT_NAME;
   public static ObjectName DEFAULTDS_WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME;

   public static ObjectName JMS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME;
   public static ObjectName JMS_MANAGED_CONNECTION_POOL_OBJECT_NAME;
   public static ObjectName JMS_CONNECTION_MANAGER_OBJECT_NAME;
   public static ObjectName JMS_CONNECTION_FACTORY_BINDING_SERVICE_OBJECT_NAME;

   public static ObjectName REMOTING_OBJECT_NAME;

   // Used only on testcases where Socket and HTTP are deployed at the same time
   public static ObjectName HTTP_REMOTING_OBJECT_NAME;

   public static ObjectName SERVER_PEER_OBJECT_NAME;

   public static String DATA_SOURCE_JNDI_NAME = "java:/DefaultDS";
   public static String TRANSACTION_MANAGER_JNDI_NAME = "java:/TransactionManager";
   public static String USER_TRANSACTION_JNDI_NAME = "UserTransaction";
   public static String JCA_JMS_CONNECTION_FACTORY_JNDI_NAME = "java:/JCAConnectionFactory";

   // Must match the value in remoting-http-service.xml
   public static long HTTP_CONNECTOR_CALLBACK_POLL_PERIOD = 102;

   // List<ObjectName>
   private List connFactoryObjectNames = new ArrayList();

   static
   {
      try
      {
         SERVICE_CONTROLLER_OBJECT_NAME =
                 new ObjectName("jboss.system:service=ServiceController");
         CLASS_LOADER_OBJECT_NAME =
                 new ObjectName("jboss.system:service=ClassLoader");
         TRANSACTION_MANAGER_OBJECT_NAME =
                 new ObjectName("jboss:service=TransactionManager");
         CACHED_CONNECTION_MANAGER_OBJECT_NAME =
                 new ObjectName("jboss.jca:service=CachedConnectionManager");

         DEFAULTDS_CONNECTION_MANAGER_OBJECT_NAME =
                 new ObjectName("jboss.jca:name=DefaultDS,service=LocalTxCM");
         DEFAULTDS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME =
                 new ObjectName("jboss.jca:name=DefaultDS,service=ManagedConnectionFactory");
         DEFAULTDS_MANAGED_CONNECTION_POOL_OBJECT_NAME =
                 new ObjectName("jboss.jca:name=DefaultDS,service=ManagedConnectionPool");
         DEFAULTDS_WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME =
                 new ObjectName("jboss.jca:name=DefaultDS,service=DataSourceBinding");

         JMS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME =
                 new ObjectName("jboss.jca:service=ManagedConnectionFactory,name=JCAConnectionFactory");
         JMS_MANAGED_CONNECTION_POOL_OBJECT_NAME =
                 new ObjectName("jboss.jca:service=ManagedConnectionPool,name=JCAConnectionFactory");
         JMS_CONNECTION_MANAGER_OBJECT_NAME =
                 new ObjectName("jboss.jca:service=TxCM,name=JCAConnectionFactory");
         JMS_CONNECTION_FACTORY_BINDING_SERVICE_OBJECT_NAME =
                 new ObjectName("jboss.jca:service=ConnectionFactoryBinding,name=JCAConnectionFactory");

         REMOTING_OBJECT_NAME =
                 new ObjectName("jboss.messaging:service=Connector,transport=bisocket");

         HTTP_REMOTING_OBJECT_NAME =
                 new ObjectName("jboss.messaging:service=Connector,transport=http");

         SERVER_PEER_OBJECT_NAME =
                 new ObjectName("jboss.messaging:service=ServerPeer");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public static String getCurrentAddress() throws Exception
   {
      String currentAddress = System.getProperty("test.bind.address");

      if (currentAddress == null)
      {
         currentAddress = "localhost";
      }
      return currentAddress;
   }

   // Attributes -----------------------------------------------------------------------------------

   private ServiceContainerConfiguration config;

   // Inner classes --------------------------------------------------------------------------------
}

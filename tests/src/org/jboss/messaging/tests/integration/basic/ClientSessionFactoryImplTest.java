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

package org.jboss.messaging.tests.integration.basic;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;

/**
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 12 dec. 2008 16:52:12
 *
 *
 */
public class ClientSessionFactoryImplTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCreateSessionFailureWithSimpleConstructorWhenNoServer() throws Exception
   {

      /****************************/
      /* No JBM Server is started */
      /****************************/

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));

      try
      {
         sf.createSession(false, true, true);
         fail("Can not create a session when there is no running JBM Server");
      }
      catch (Exception e)
      {
      }

   }

   /**
    * The test is commented because it generates an infinite loop.
    * The configuration to have it occured is:
    * - no backup & default values for max retries before/after failover
    * 
    * - The infinite loop is in ConnectionManagerImpl.getConnectionForCreateSession()
    *   - getConnection(1) always return null (no server to connect to)
    *   - failover() always return true
    *        - the value returned by failover() comes from the reconnect() method
    *        - when there is no session already connected, the reconnect() method does *nothing* 
    *          and returns true (while nothing has been reconnected)
    */
   public void _testCreateSessionFailureWithDefaultValuesWhenNoServer() throws Exception
   {

      /****************************/
      /* No JBM Server is started */
      /****************************/

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()),
                                                             null,
                                                             ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL,
                                                             ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

      try
      {
         sf.createSession(false, true, true);
         fail("Can not create a session when there is no running JBM Server");
      }
      catch (Exception e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

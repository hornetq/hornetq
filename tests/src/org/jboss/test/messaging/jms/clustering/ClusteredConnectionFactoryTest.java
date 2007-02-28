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

package org.jboss.test.messaging.jms.clustering;

import java.lang.reflect.Field;

import javax.jms.Connection;

import org.jboss.jms.client.ClientAOPStackLoader;
import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.aop.PoisonInterceptor;

/**
 * TODO: Is it possible to move tests I added on ClusteredConnectionFactoryGraveyardTest?
 * TODO:      These tests are kiilling server 0, as AOP is loaded starting from server 0.
 * TODO:      and I wanted to validate crashing scenarios
 * TODO: It's possible to run them manually but not as part of the entire testsuite.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class ClusteredConnectionFactoryTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClusteredConnectionFactoryTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testGetAOPBroken() throws Exception
   {
      Connection conn = null;

      try
      {

         resetAOP();

         ServerManagement.killAndWait(2);
         ServerManagement.killAndWait(1);
         ServerManagement.killAndWait(0);

         try
         {
            conn = cf.createConnection();
            fail ("This should try an exception as every server is down");
         }
         catch (RuntimeException e)
         {
            log.error(e.toString(), e);
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         // need to re-start 0, it's the RMI server the other servers use
         ServerManagement.start(0, "all", true);
      }
   }

   // TODO: Commented out pending resolution.
   // See http://jira.jboss.org/jira/browse/JBMESSAGING-900
//   public void testGetAOPBounce() throws Exception
//   {
//      Connection conn = null;
//      Server poisonedServer = null;
//
//      try
//      {
//
//         resetAOP();
//
//         ServerManagement.killAndWait(0);
//         poisonedServer =
//            ServerManagement.poisonTheServer(1, PoisonInterceptor.CF_GET_CLIENT_AOP_STACK);
//
//         conn = cf.createConnection();
//         assertEquals(2, ((JBossConnection)conn).getServerID());
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//
//         // need to re-start 0, it's the RMI server the other servers use
//         ServerManagement.start(0, "all", true);
//
//         // Kill the poisoned server
//         if (poisonedServer != null)
//         {
//            poisonedServer.kill();
//         }
//      }
//   }

   public void testCreateConnectionOnBrokenServer() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         assertEquals(0, ((JBossConnection)conn).getServerID());
         conn.close();
         conn = null;

         ServerManagement.killAndWait(1);
         conn = cf.createConnection();

         assertEquals(2,((JBossConnection)conn).getServerID());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testPoisonCFs() throws Exception
   {
      Connection conn = null;

      try
      {

         // Poison each server with a different pointcut crash
         ServerManagement.poisonTheServer(1, PoisonInterceptor.CF_CREATE_CONNECTION);

         conn = cf.createConnection();
         assertEquals(0, ((JBossConnection)conn).getServerID());
         conn.close();
         conn = null;

         // this should break on server1
         log.info("creating connection on server 1");
         conn = cf.createConnection();

         assertEquals(2, ((JBossConnection)conn).getServerID());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testCreateConnectionTwoServersBroken() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         assertEquals(0, ((JBossConnection)conn).getServerID());
         conn.close();

         ServerManagement.killAndWait(1);
         ServerManagement.killAndWait(2);
         conn = cf.createConnection();

         assertEquals(0, ((JBossConnection)conn).getServerID());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testFailureOnGetBlockId() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         assertEquals(0, ((JBossConnection)conn).getServerID());
         conn.close();

         ServerManagement.killAndWait(1);
         ServerManagement.killAndWait(2);
         conn = cf.createConnection();

         assertEquals(0, ((JBossConnection)conn).getServerID());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 3;
      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   private void resetAOP() throws NoSuchFieldException, IllegalAccessException
   {

      // Using reflection to cleanup AOP status and force to load AOP again
      Field field = ClientAOPStackLoader.class.getDeclaredField("instance");
      field.setAccessible(true);
      log.info("Reseting AOP");
      field.set(null, null);
   }

   // Inner classes --------------------------------------------------------------------------------

}

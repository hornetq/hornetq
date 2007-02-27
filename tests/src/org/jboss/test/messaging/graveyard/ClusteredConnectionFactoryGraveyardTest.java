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

package org.jboss.test.messaging.graveyard;

import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.aop.PoisonInterceptor;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.ClientAOPStackLoader;
import javax.jms.Connection;
import java.lang.reflect.Field;

/**
 * There are tests that should be on ClusteredConnectionFActoryTest that will need to kill server0
 * to validate AOP configs.
 *
 * However it's not allowed to kill these servers under our regular scenario.
 *
 * So.. I'm adding these tests here to the graveyard, so we could run them manually
 * until we figure out how to resurrect them.. what means.. how to avoid/allow killing server0 
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class ClusteredConnectionFactoryGraveyardTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClusteredConnectionFactoryGraveyardTest(String name)
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
      }
   }

   public void testGetAOPBounce() throws Exception
   {
      Connection conn = null;

      try
      {

         resetAOP();

         ServerManagement.killAndWait(0);
         ServerManagement.poisonTheServer(1, PoisonInterceptor.CF_GET_CLIENT_AOP_STACK);

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



   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 3;
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   private void resetAOP()
      throws NoSuchFieldException, IllegalAccessException
   {

      // Using reflection to cleanup AOP status and force to load AOP again
      Field field = ClientAOPStackLoader.class.getDeclaredField("instance");
      field.setAccessible(true);
      log.info("Reseting AOP");
      field.set(null,null);
   }


   
   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}

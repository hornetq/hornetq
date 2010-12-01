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
package org.hornetq.tests.integration.client;

import java.lang.ref.WeakReference;

import junit.framework.Assert;

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A SessionCloseOnGCTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <mailto:tim.fox@jboss.org">Tim Fox</a>
 *
 *
 */
public class SessionCloseOnGCTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(SessionCloseOnGCTest.class);

   private HornetQServer server;

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);
      server.start();

      locator = createInVMNonHALocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      //locator.close();
      
      server.stop();

      server = null;

      super.tearDown();
   }

   /** Make sure Sessions are not leaking after closed..
    *  Also... we want to make sure the SessionFactory will close itself when there are not references into it */
   public void testValidateFactoryGC1() throws Exception
   {
      try
      {
         ClientSessionFactory factory = locator.createSessionFactory();

         ClientSession s1 = factory.createSession();
         ClientSession s2 = factory.createSession();

         s1.close();
         s2.close();

         WeakReference<ClientSession> wrs1 = new WeakReference<ClientSession>(s1);
         WeakReference<ClientSession> wrs2 = new WeakReference<ClientSession>(s2);

         s1 = null;
         s2 = null;

         locator.close();

         locator = null;
         UnitTestCase.checkWeakReferences(wrs1, wrs2);

         WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

         factory.close();

         factory = null;

         UnitTestCase.checkWeakReferences(fref, wrs1, wrs2);
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {

         }
      }
   }

   public void testValidateFactoryGC2() throws Exception
   {
      try
      {

         locator.setUseGlobalPools(false);

         ClientSessionFactory factory = locator.createSessionFactory();

         ClientSession s1 = factory.createSession();
         ClientSession s2 = factory.createSession();

         s1.close();
         s2.close();

         WeakReference<ClientSession> wrs1 = new WeakReference<ClientSession>(s1);
         WeakReference<ClientSession> wrs2 = new WeakReference<ClientSession>(s2);

         s1 = null;
         s2 = null;

         locator.close();

         locator = null;
         UnitTestCase.checkWeakReferences(wrs1, wrs2);

         WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

         factory.close();

         factory = null;

         UnitTestCase.checkWeakReferences(fref, wrs1, wrs2);
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {

         }
      }
   }

   public void testValidateFactoryGC3() throws Exception
   {
      try
      {
         ClientSessionFactory factory = locator.createSessionFactory();

         ClientSession s1 = factory.createSession();
         ClientSession s2 = factory.createSession();

         s1.close();
         s2.close();

         WeakReference<ClientSession> wrs1 = new WeakReference<ClientSession>(s1);
         WeakReference<ClientSession> wrs2 = new WeakReference<ClientSession>(s2);

         s1 = null;
         s2 = null;

         locator.close();

         locator = null;
         UnitTestCase.checkWeakReferences(wrs1, wrs2);

         WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

         factory = null;

         UnitTestCase.checkWeakReferences(fref, wrs1, wrs2);
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {

         }
      }
   }

   public void testValidateFactoryGC4() throws Exception
   {
      try
      {
         ClientSessionFactory factory = locator.createSessionFactory();

         ClientSession s1 = factory.createSession();
         ClientSession s2 = factory.createSession();

         WeakReference<ClientSession> wrs1 = new WeakReference<ClientSession>(s1);
         WeakReference<ClientSession> wrs2 = new WeakReference<ClientSession>(s2);

         s1 = null;
         s2 = null;

         locator.close();

         locator = null;
         UnitTestCase.checkWeakReferences(wrs1, wrs2);

         WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

         factory = null;

         UnitTestCase.checkWeakReferences(fref, wrs1, wrs2);
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {

         }
      }
   }

   public void testValidateFactoryGC5() throws Exception
   {
      try
      {
         ClientSessionFactory factory = locator.createSessionFactory();

         WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

         factory = null;

         locator.close();

         locator = null;
         UnitTestCase.checkWeakReferences(fref);
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {

         }
      }
   }

   public void testCloseOneSessionOnGC() throws Exception
   {
      ClientSessionFactoryImpl sf = (ClientSessionFactoryImpl) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      WeakReference<ClientSession> wses = new WeakReference<ClientSession>(session);

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      session = null;

      UnitTestCase.checkWeakReferences(wses);

      Assert.assertEquals(0, sf.numSessions());
      Assert.assertEquals(1, sf.numConnections());
      Assert.assertEquals(1, server.getRemotingService().getConnections().size());
   }

   public void testCloseSeveralSessionOnGC() throws Exception
   {
      ClientSessionFactoryImpl sf = (ClientSessionFactoryImpl) locator.createSessionFactory();

      ClientSession session1 = sf.createSession(false, true, true);
      ClientSession session2 = sf.createSession(false, true, true);
      ClientSession session3 = sf.createSession(false, true, true);

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      WeakReference<ClientSession> ref1 = new WeakReference<ClientSession>(session1);
      WeakReference<ClientSession> ref2 = new WeakReference<ClientSession>(session2);
      WeakReference<ClientSession> ref3 = new WeakReference<ClientSession>(session3);

      session1 = null;
      session2 = null;
      session3 = null;

      UnitTestCase.checkWeakReferences(ref1, ref2, ref3);

      Assert.assertEquals(0, sf.numSessions());
      Assert.assertEquals(1, sf.numConnections());
      Assert.assertEquals(1, server.getRemotingService().getConnections().size());
   }

}

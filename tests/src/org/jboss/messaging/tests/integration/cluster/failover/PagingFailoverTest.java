/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.tests.integration.cluster.failover;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnector;
import org.jboss.messaging.utils.SimpleString;

/**
 * A PagingFailoverTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 5, 2009 10:57:42 AM
 *
 *
 */
public class PagingFailoverTest extends FailoverTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   protected static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFailoverOnPaging() throws Exception
   {
      testPaging(true);
   }

   public void testReplicationOnPaging() throws Exception
   {
      testPaging(false);
   }

   private void testPaging(final boolean fail) throws Exception
   {
      setUpFileBased(100 * 1024);

      ClientSession session = null;
      try
      {
         ClientSessionFactory sf1 = createFailoverFactory();

         session = sf1.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true, false);

         ClientProducer producer = session.createProducer(ADDRESS);

         final int numMessages = getNumberOfMessages();

         PagingManager pmLive = liveService.getServer().getPostOffice().getPagingManager();
         PagingStore storeLive = pmLive.getPageStore(ADDRESS);

         PagingManager pmBackup = backupService.getServer().getPostOffice().getPagingManager();
         PagingStore storeBackup = pmBackup.getPageStore(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session.createClientMessage(true);
            message.getBody().writeInt(i);

            producer.send(message);

            if (storeLive.isPaging())
            {
               assertTrue(storeBackup.isPaging());
            }
         }

         session.close();
         session = sf1.createSession(null, null, false, true, true, false, 0);
         session.start();

         final RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

         assertEquals("GloblSize", pmLive.getGlobalSize(), pmBackup.getGlobalSize());

         assertEquals("PageSizeLive", storeLive.getAddressSize(), pmLive.getGlobalSize());

         assertEquals("PageSizeBackup", storeBackup.getAddressSize(), pmBackup.getGlobalSize());

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         for (int i = 0; i < numMessages; i++)
         {

            if (fail && i == numMessages / 2)
            {
               conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
            }

            ClientMessage message = consumer.receive(10000);


            assertNotNull(message);

            message.acknowledge();

            assertEquals(i, message.getBody().readInt());

         }

         session.close();
         session = null;

         if (!fail)
         {
            assertEquals(0, pmLive.getGlobalSize());
            assertEquals(0, storeLive.getAddressSize());
         }
         assertEquals(0, pmBackup.getGlobalSize());
         assertEquals(0, storeBackup.getAddressSize());

      }
      finally
      {
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (Exception ignored)
            {
               // eat it
            }
         }
      }

   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected int getNumberOfMessages()
   {
      return 500;
   }
   
   protected void fail(final ClientSession session) throws Exception
   {
      RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session).getConnection();

      InVMConnector.numberOfFailures = 1;
      InVMConnector.failOnCreateConnection = true;
      System.out.println("Forcing a failure");
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));

   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

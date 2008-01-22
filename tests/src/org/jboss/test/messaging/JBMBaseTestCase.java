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
package org.jboss.test.messaging;

import java.lang.ref.WeakReference;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.client.JBossConnection;
import org.jboss.messaging.microcontainer.JBMBootstrapServer;
import org.jboss.messaging.util.Logger;
import org.jboss.test.messaging.util.ProxyAssertSupport;
import org.jboss.tm.TransactionManagerLocator;

/**
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JBMBaseTestCase extends ProxyAssertSupport
{
   private JBMBootstrapServer bootstrap;


   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Logger log = Logger.getLogger(getClass());

   // Constructors --------------------------------------------------

   public JBMBaseTestCase()
   {
      super();    //To change body of overridden methods use File | Settings | File Templates.
   }

   public JBMBaseTestCase(String string)
   {
      super(string);    //To change body of overridden methods use File | Settings | File Templates.
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      System.setProperty("java.naming.factory.initial", getContextFactory());
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   protected List getMessageIds() throws Exception
   {
      InitialContext ctx = getInitialContext();

      TransactionManager mgr = TransactionManagerLocator.locateTransactionManager();
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");

      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      java.sql.Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGE_ID FROM JBM_MSG ORDER BY MESSAGE_ID";
      PreparedStatement ps = conn.prepareStatement(sql);

      ResultSet rs = ps.executeQuery();

      List msgIds = new ArrayList();

      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }

      return msgIds;
   }

    protected List getReferenceIds() throws Exception
   {
      InitialContext ctx = getInitialContext();

      TransactionManager mgr = TransactionManagerLocator.locateTransactionManager();
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");

      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      java.sql.Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGE_ID, ORD FROM JBM_MSG_REF";
      PreparedStatement ps = conn.prepareStatement(sql);

      ResultSet rs = ps.executeQuery();

      List msgIds = new ArrayList();

      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }

      return msgIds;
   }


   protected List getReferenceIds(long channelId) throws Throwable
   {
      InitialContext ctx = getInitialContext();

      TransactionManager mgr = TransactionManagerLocator.locateTransactionManager();
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");

      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      java.sql.Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGE_ID FROM JBM_MSG_REF WHERE CHANNEL_ID=? ORDER BY ORD";
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setLong(1, channelId);

      ResultSet rs = ps.executeQuery();

      List msgIds = new ArrayList();

      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }

      return msgIds;
   }

   protected boolean checkNoBindingData() throws Exception
   {
      InitialContext ctx = getInitialContext();

      TransactionManager mgr = TransactionManagerLocator.locateTransactionManager();
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");

      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      java.sql.Connection conn = null;

      PreparedStatement ps = null;

      ResultSet rs = null;

      try
      {
         conn = ds.getConnection();
         String sql = "SELECT * FROM JBM_POSTOFFICE";
         ps = conn.prepareStatement(sql);

         rs = ps.executeQuery();

         return rs.next();
      }
      finally
      {
         if (rs != null) rs.close();

         if (ps != null) ps.close();

         if (conn != null) conn.close();

         mgr.commit();

         if (txOld != null)
         {
            mgr.resume(txOld);
         }

      }
   }

   protected XAConnection createXAConnectionOnServer(XAConnectionFactory factory, int serverId)
   throws Exception
   {
   	int count=0;

   	while (true)
   	{
   		if (count++>10)
   			return null;

   		XAConnection connection = factory.createXAConnection();

   		if (getServerId(connection) == serverId)
   		{
   			return connection;
   		}
   		else
   		{
   			connection.close();
   		}
   	}
   }

   /**
       * @param conn a JMS connection
       * @return the ID of the ServerPeer the connection is communicating with.
       */
      protected static int getServerId(Connection conn)
      {
         return ((JBossConnection) conn).getServerID();
      }
   protected Connection createConnectionOnServer(ConnectionFactory factory, int serverId)
      throws Exception
      {
         int count=0;

         while (true)
         {
            if (count++>10)
               throw new IllegalStateException("Cannot make connection to node " + serverId);

            Connection connection = factory.createConnection();

            if (getServerId(connection) == serverId)
            {
               return connection;
            }
            else
            {
               connection.close();
            }
         }
      }

      protected Connection createConnectionOnServer(ConnectionFactory factory, int serverId, String user, String password)
      throws Exception
      {
         int count=0;

         while (true)
         {
            if (count++>10)
               throw new IllegalStateException("Cannot make connection to node " + serverId);

            Connection connection = factory.createConnection(user, password);

            if (getServerId(connection) == serverId)
            {
               return connection;
            }
            else
            {
               connection.close();
            }
         }
      }

   protected void checkNoSubscriptions(Topic topic) throws Exception
     {

     }

     protected void checkNoSubscriptions(Topic topic, int server) throws Exception
     {
        
     }

   public InitialContext getInitialContext() throws Exception
   {
      Properties props = new Properties();
      props.setProperty("java.naming.factory.initial", getContextFactory());
      //props.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
      return new InitialContext(props);
   }

   public String getContextFactory()
   {
      return "org.jboss.test.messaging.tools.container.InVMInitialContextFactory";
   }
   protected void drainDestination(ConnectionFactory cf, Destination dest) throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection();
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(dest);
         Message m = null;
         conn.start();
         log.trace("Draining messages from " + dest);
         while (true)
         {
            m = cons.receive(500);
            if (m == null) break;
            log.trace("Drained message");
         }
      }
      finally
      {
         if (conn!= null) conn.close();
      }
   }

   /** Some testcases are time sensitive, and we need to make sure a GC would happen before certain scenarios*/
   protected void forceGC()
   {
      WeakReference dumbReference = new WeakReference(new Object());
      // A loopt that will wait GC, using the minimal time as possible
      while (dumbReference.get() != null)
      {
         System.gc();
         try
         {
            Thread.sleep(500);
         } catch (InterruptedException e)
         {
         }
      }
   }
}

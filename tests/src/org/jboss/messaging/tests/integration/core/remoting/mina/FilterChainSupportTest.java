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

package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addCodecFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addSSLFilter;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import javax.net.ssl.SSLException;

import junit.framework.TestCase;

import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.ssl.SslFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class FilterChainSupportTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String keystorePath;
   private String keystorePassword;
   private String trustStorePath;
   private String trustStorePassword;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      keystorePath = "messaging.keystore";
      keystorePassword = "secureexample";
      trustStorePath = "messaging.truststore";
      trustStorePassword = keystorePassword;
   }

   public void testSSLFilter() throws Exception
   {
      InetSocketAddress address = new InetSocketAddress("localhost", 9091);
      NioSocketAcceptor acceptor = new NioSocketAcceptor();
      addSSLFilter(acceptor.getFilterChain(), false, keystorePath,
            keystorePassword, trustStorePath, trustStorePassword);
      addCodecFilter(acceptor.getFilterChain());
      acceptor.setDefaultLocalAddress(address);

      final CountDownLatch latch = new CountDownLatch(1);
      
      acceptor.setHandler(new IoHandlerAdapter()
      {
         @Override
         public void messageReceived(IoSession session, Object message)
               throws Exception
         {
            latch.countDown();
         }
      });
      acceptor.bind();

      NioSocketConnector connector = new NioSocketConnector();
      addSSLFilter(connector.getFilterChain(), true,
            keystorePath, keystorePassword, null, null);
      addCodecFilter(connector.getFilterChain());
      connector.setHandler(new IoHandlerAdapter());
      ConnectFuture future = connector.connect(address).awaitUninterruptibly();
      IoSession session = future.getSession();
      session.write(new Ping(randomLong()));

      boolean gotMessage = latch.await(500, MILLISECONDS);
      assertTrue(gotMessage);
      
      SslFilter sslFilter =  ((SslFilter)session.getFilterChain().get("ssl"));
      if (sslFilter != null)
      {
         try
         {
            sslFilter.stopSsl(session);
         } catch (SSLException e)
         {
            fail(e.getMessage());
         }
      }
      
      boolean sessionClosed = session.close().await(500, MILLISECONDS);
      assertTrue(sessionClosed);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

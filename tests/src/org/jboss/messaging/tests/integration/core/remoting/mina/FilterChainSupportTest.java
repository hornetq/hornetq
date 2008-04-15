/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addCodecFilter;
import static org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport.addSSLFilter;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import javax.net.ssl.SSLException;

import junit.framework.TestCase;

import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.ssl.SslFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.impl.mina.FailureNotifier;
import org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport;
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

   public void testAddKeepAliveFilterWithIncorrectParameters() throws Exception
   {
      int keepAliveInterval = 5; // seconds
      int keepAliveTimeout = 10; // seconds

      DefaultIoFilterChainBuilder filterChain = new DefaultIoFilterChainBuilder();
      KeepAliveFactory factory = createMock(KeepAliveFactory.class);
      FailureNotifier notifier = createMock(FailureNotifier.class);

      replay(factory, notifier);

      try
      {
         FilterChainSupport.addKeepAliveFilter(filterChain, factory,
               keepAliveInterval, keepAliveTimeout, notifier);
         fail("the interval must be greater than the timeout");
      } catch (IllegalArgumentException e)
      {
      }

      verify(factory, notifier);
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

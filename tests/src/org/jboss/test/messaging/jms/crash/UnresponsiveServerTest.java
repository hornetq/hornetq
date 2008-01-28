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
package org.jboss.test.messaging.jms.crash;

import static java.lang.Boolean.TRUE;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.core.remoting.impl.mina.MinaService.DISABLE_INVM_KEY;
import static org.jboss.messaging.core.remoting.impl.mina.MinaService.KEEP_ALIVE_INTERVAL_KEY;
import static org.jboss.messaging.core.remoting.impl.mina.MinaService.KEEP_ALIVE_TIMEOUT_KEY;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.KEEP_ALIVE_INTERVAL;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.KEEP_ALIVE_TIMEOUT;

import java.util.HashMap;
import java.util.Map;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;

import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.impl.mina.ServerKeepAliveFactory;
import org.jboss.test.messaging.jms.JMSTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class UnresponsiveServerTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   private MinaService minaService;
   private Map<String, String> originalParameters;


   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public UnresponsiveServerTest(String name)
   {
      super(name);
   }
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
         
      minaService = servers.get(0).getMessagingServer().getMinaService();
      originalParameters = new HashMap<String, String>(minaService.getLocator().getParameters());
   }

   @Override
   protected void tearDown() throws Exception
   {
      minaService.stop();
      minaService.setParameters(originalParameters);
      minaService.setKeepAliveFactory(new ServerKeepAliveFactory());
      minaService.start();

      super.tearDown();
   }

   // Public --------------------------------------------------------

   public void testExceptionListenerWhenServerIsUnresponsive()
         throws Exception
   {
      KeepAliveFactory factory = createMock(KeepAliveFactory.class);
      // server does not send ping
      expect(factory.ping()).andStubReturn(null);
      // no pong -> server is not responding
      expect(factory.pong()).andReturn(null).atLeastOnce();

      ExceptionListener listener = createMock(ExceptionListener.class);
      listener.onException(isA(JMSException.class));
      expectLastCall().once();

      replay(listener, factory);
      
      minaService.stop();
      Map<String, String> parameters = new HashMap<String, String>();
      parameters.put(KEEP_ALIVE_INTERVAL_KEY, Integer.toString(KEEP_ALIVE_INTERVAL));
      parameters.put(KEEP_ALIVE_TIMEOUT_KEY, Integer.toString(KEEP_ALIVE_TIMEOUT));
      parameters.put(DISABLE_INVM_KEY, TRUE.toString());
      minaService.setParameters(parameters);
      minaService.setKeepAliveFactory(factory);
      minaService.start();
      
      QueueConnection conn = getConnectionFactory().createQueueConnection();
      conn.setExceptionListener(listener);

      // FIXME should deduce them from MinaConnector somehow...
      Thread.sleep((5 + 10 + 1) * 1000);
      
      verify(listener, factory);
      
      conn.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

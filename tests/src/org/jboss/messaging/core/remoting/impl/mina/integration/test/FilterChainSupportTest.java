/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import junit.framework.TestCase;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.impl.mina.FailureNotifier;
import org.jboss.messaging.core.remoting.impl.mina.FilterChainSupport;

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

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

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
               keepAliveInterval, keepAliveTimeout,  notifier);
         fail("the interval must be greater than the timeout");
      } catch (IllegalArgumentException e)
      {
      }
      
      verify(factory, notifier);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;


import java.io.Serializable;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.delegate.BrowserDelegate;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * 
 * Interceptor that caches blocks of messages during queue browsing these allows us to cache blocks
 * of browseable messages in the interceptor thus preventing excessive network traffic.
 * 
 * Important! There should be *one instance* of this interceptor per instance of QueueBrowser
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 */
public class BrowserInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 3694874918265592846L;
   
   //TODO - these need to be configurable by the user - should be configured from jboss-aop.xml
   
   //FIXME - Batching isn't currently working properly
   private static final boolean BATCH_MESSAGES = false;
   private static final int MSG_BLOCK_SIZE = 5;
   
   // Attributes ----------------------------------------------------
   
   private Message[] cache;
   private int pos;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ----------------------------------
   
   public String getName()
   {
      return "BrowserInterceptor";
   }
   
   public Object invoke(Invocation invocation) throws Throwable
   {      
      if (!BATCH_MESSAGES)
      {
         return invocation.invokeNext();
      }
      
      String methodName = ((MethodInvocation) invocation).getMethod().getName();
      
      if ("nextMessage".equals(methodName))
      {
         checkCache(invocation);
         Message mess = cache[pos++];
         if (pos == cache.length)
         {
            cache = null;
         }
         return mess;
      }
      else if ("hasNextMessage".equals(methodName))
      {
         if (cache != null)
         {
            return Boolean.TRUE;
         }
         return invocation.invokeNext();
      }

      return invocation.invokeNext();
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private void checkCache(Invocation invocation) throws JMSException
   {
      if (cache == null)
      {
         BrowserDelegate bd = getDelegate(invocation);
         cache = bd.nextMessageBlock(MSG_BLOCK_SIZE);
         pos = 0;
      }
   }
   
   private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }
   
   private BrowserDelegate getDelegate(Invocation invocation)
   {
      return (BrowserDelegate)getHandler(invocation).getDelegate();
   }
   
   // Inner Classes -------------------------------------------------
   
   
	
	

}


/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.jboss.aop.MethodJoinPoint;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.MethodHashing;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.logging.Logger;



/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a> Adapted to maintain hierarchy
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSInvocationHandler implements InvocationHandler, Serializable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSInvocationHandler.class);

   private final static long serialVersionUID = 1945932848345348326L;

   // Static --------------------------------------------------------
 
   // Attributes ----------------------------------------------------

   protected Interceptor[] interceptors;

   // metadate to be transferred to the invocation when it is created. Usually contains oids (the
   // id used to locate the right Advisor in the Dispatcher's map), remoting locators, etc.
   protected SimpleMetaData metadata;
   
   protected JMSInvocationHandler parent;
   
   protected Set children = Collections.synchronizedSet(new HashSet());
   
   protected Object delegate;

   // Constructors --------------------------------------------------

   public JMSInvocationHandler(Interceptor[] interceptors)
   {
      this.interceptors = interceptors;      
   }

   // Public --------------------------------------------------------

   public SimpleMetaData getMetaData()
   {
      synchronized (this)
      {
         if (metadata == null)
         {
            metadata = new SimpleMetaData();
         }
      }
      return metadata;
   }

   // InvocationHandler implementation ------------------------------

   public Object invoke(Object proxy, Method method,  Object[] args) throws Throwable
   {
      // build the invocation

      long hash = MethodHashing.calculateHash(method);

      MethodJoinPoint info = new MethodJoinPoint();
      info.hash = hash;
      info.advisedMethod = method;
      info.unadvisedMethod = method;

      MethodInvocation invocation = new JMSMethodInvocation(info, interceptors, this);

      invocation.setArguments(args);
     
      // initialize the invocation's metadata
      // TODO I don't know if this is the most efficient thing to do
      invocation.getMetaData().mergeIn(getMetaData());

      if (log.isTraceEnabled()) { log.trace("invocation (" + invocation.getMethod().getName() + ") submitted to the interceptor chain"); }

      return invocation.invokeNext();
   }
   
   // Package protected ---------------------------------------------

   JMSInvocationHandler getParent()
   {
       return parent;
   }

   void setParent(JMSInvocationHandler parent)
   {
      this.parent = parent;
   }

   Set getChildren()
   {
       return children;
   }

   void addChild(JMSInvocationHandler child)
   {
       children.add(child);
       child.setParent(this);
   }

   void removeChild(JMSInvocationHandler child)
   {
       children.remove(child);
       child.setParent(null);
   }

   Object getDelegate()
   {
       return delegate;
   }

   void setDelegate(Object proxy)
   {
       this.delegate = proxy;
   }

   /**
    * Returns the most significant ID maintained by the handler's metadata for this specific
    * handler instance.
    */
   Object getDelegateID()
   {
      Object id = metadata.getMetaData(JMSAdvisor.JMS, JMSAdvisor.BROWSER_ID);
      if (id == null)
      {
         id = metadata.getMetaData(JMSAdvisor.JMS, JMSAdvisor.PRODUCER_ID);
      }
      if (id == null)
      {
         id = metadata.getMetaData(JMSAdvisor.JMS, JMSAdvisor.CONSUMER_ID);
      }
      if (id == null)
      {
         id = metadata.getMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID);
      }
      if (id == null)
      {
         id = metadata.getMetaData(JMSAdvisor.JMS, JMSAdvisor.CONNECTION_ID);
      }
      return id;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

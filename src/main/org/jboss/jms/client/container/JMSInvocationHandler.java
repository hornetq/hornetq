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
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.MethodHashing;

import org.jboss.aop.MethodJoinPoint;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;



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

      return invocation.invokeNext();
   }
   
   
   //tim's stuff
   
   public JMSInvocationHandler getParent()
   {
       return parent;
   }
   
   public void setParent(JMSInvocationHandler parent)
   {
      this.parent = parent;
   }
   
   public Set getChildren()
   {
       return children;
   }
   
   public void addChild(JMSInvocationHandler child)
   {
       children.add(child);
       child.setParent(this);
   }
   
   public void removeChild(JMSInvocationHandler child)
   {
       children.remove(child);
       child.setParent(null);
   }
   
   public Object getDelegate()
   {
       return delegate;
   }
   
   public void setDelegate(Object proxy)
   {
       this.delegate = proxy;
   }
	

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

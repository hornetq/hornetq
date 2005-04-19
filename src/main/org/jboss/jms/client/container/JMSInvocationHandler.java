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

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
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

      MethodInvocation invocation = new MethodInvocation(interceptors, hash, method, method, null);
      invocation.setArguments(args);

      // initialize the invocation's metadata
      // TODO I don't know if this is the most efficient thing to do
      invocation.getMetaData().mergeIn(getMetaData());

      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

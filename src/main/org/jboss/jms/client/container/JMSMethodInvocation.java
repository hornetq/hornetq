/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.MethodJoinPoint;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.advice.Interceptor;

/**
 * Sub-class of MethodInvocation that allows the InvocationHandler to be retrieved
 * from the MethodInvocation.
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * 
 */
public class JMSMethodInvocation extends MethodInvocation
{   
	private static final long serialVersionUID = -4217789262697969755L;

   private transient JMSInvocationHandler handler;

   /** Externalizable classes should have a no-argument constructor **/
   public JMSMethodInvocation()
   {                                                                         
   }

   public JMSMethodInvocation(MethodJoinPoint info, Interceptor[] interceptors, JMSInvocationHandler handler)
   {
      super(info, interceptors);
      this.handler = handler;
   }
   
  
   
   JMSInvocationHandler getHandler()
   {
      return handler;
   }
}

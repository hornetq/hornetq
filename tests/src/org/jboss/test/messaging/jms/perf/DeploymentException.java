/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DeploymentException extends Exception
{
   private static final long serialVersionUID = -2051237190722275283L;

   public DeploymentException(String s)
   {
      super(s);
   }
}

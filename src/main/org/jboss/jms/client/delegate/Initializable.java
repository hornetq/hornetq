/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.delegate;

/**
 * Applies to delegates.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Initializable
{
   /**
    * Usually add the delegate itself as the last invoking interceptor in the stack, and prepare
    * the stack for invocations.
    */
   void init();
}

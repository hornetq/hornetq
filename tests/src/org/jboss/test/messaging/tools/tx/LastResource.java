/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.tx;

/**
 * A tagging interface to identify an XAResource that does not support prepare and should be used
 * in the last resource gambit. i.e. It is committed after the resources are prepared. If it fails
 * to commit, roll everybody back.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
interface LastResource
{
}


/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.refqueue;

/**
 * 
 * A Node in a Deque
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 */
public interface Node
{
   void remove();
   
   Object getObject();
}

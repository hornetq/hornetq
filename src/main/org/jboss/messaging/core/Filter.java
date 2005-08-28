/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;


/**
 * A filter encapsulates the logic of whether to accept a message or not. Filters are used when
 * browsing to restrict the messages browsed, or when routing messages.
 *
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Filter
{
	/**
	 * Tests whether the routable should be accepted.
    *
	 * @return true if the Filter accepts the routable - i.e. let's it pass.
	 */
	public boolean accept(Routable routable);
}

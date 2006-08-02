/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.rmi;


import java.rmi.Remote;

import org.jboss.jms.perf.framework.remoting.Request;
import org.jboss.jms.perf.framework.remoting.Result;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface Server extends Remote
{
   Result execute(Request request) throws Exception;
}

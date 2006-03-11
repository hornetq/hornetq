/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.rmi;


import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.remoting.Request;

import java.rmi.Remote;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface Server extends Remote
{
   Result execute(Request request) throws Exception;
}

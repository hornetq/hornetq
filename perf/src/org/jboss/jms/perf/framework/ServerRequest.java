/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import java.io.Serializable;

/**
 * 
 * A ServerRequest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public interface ServerRequest extends Serializable
{
   Object execute(JobStore store) throws Exception;
}

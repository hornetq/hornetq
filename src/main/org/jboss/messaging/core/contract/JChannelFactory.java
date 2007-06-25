package org.jboss.messaging.core.contract;

import org.jgroups.JChannel;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface JChannelFactory
{
   JChannel createSyncChannel() throws Exception;
   
   JChannel createASyncChannel() throws Exception;
}

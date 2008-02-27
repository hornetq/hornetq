/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.asyncio;

/**
 * 
 * @author clebert.suconic@jboss.com
 *
 */
public interface AIOCallback
{
    /** Leave this method as soon as possible, or you would be blocking the whole notification thread */
    void done();

    /** Observation: The whole file will be probably failing if this happens. Like, if you delete the file, you will start to get errors for these operations*/
    void onError(int errorCode, String errorMessage);
}

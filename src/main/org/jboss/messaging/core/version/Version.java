/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.version;

/**
 * 
 * A VersionImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Version
{
	String getFullVersion();
	
	String getVersionName();
	
	int getMajorVersion();
	
	int getMinorVersion();
	
	int getMicroVersion();
	
	String getVersionSuffix();
	
	int getIncrementingVersion();
}

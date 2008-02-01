package org.jboss.messaging.core;

/**
 * Used when merging objects together.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface Mergeable<T>
{
   void merge(T merged);
}

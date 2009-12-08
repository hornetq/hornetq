/*
 * Written by Doug Lea and Josh Bloch with assistance from members of
 * JCP JSR-166 Expert Group and released to the public domain, as explained
 * at http://creativecommons.org/licenses/publicdomain
 */

package org.hornetq.utils.concurrent; // XXX This belongs in java.util!!! XXX

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Deque} that additionally supports operations that wait for
 * the deque to become non-empty when retrieving an element, and wait
 * for space to become available in the deque when storing an
 * element. These methods are summarized in the following table:<p>
 *
 * <table BORDER CELLPADDING=3 CELLSPACING=1>
 *  <tr>
 *    <td></td>
 *    <td ALIGN=CENTER COLSPAN = 2> <b>First Element (Head)</b></td>
 *    <td ALIGN=CENTER COLSPAN = 2> <b>Last Element (Tail)</b></td>
 *  </tr>
 *  <tr>
 *    <td></td>
 *    <td ALIGN=CENTER><em>Block</em></td>
 *    <td ALIGN=CENTER><em>Time out</em></td>
 *    <td ALIGN=CENTER><em>Block</em></td>
 *    <td ALIGN=CENTER><em>Time out</em></td>
 *  </tr>
 *  <tr>
 *    <td><b>Insert</b></td>
 *    <td>{@link #putFirst putFirst(e)}</td>
 *    <td>{@link #offerFirst(Object, long, TimeUnit) offerFirst(e, time, unit)}</td>
 *    <td>{@link #putLast putLast(e)}</td>
 *    <td>{@link #offerLast(Object, long, TimeUnit) offerLast(e, time, unit)}</td> 
 *  </tr>
 *  <tr>
 *    <td><b>Remove</b></td>
 *    <td>{@link #takeFirst takeFirst()}</td>
 *    <td>{@link #pollFirst(long, TimeUnit)  pollFirst(time, unit)}</td>
 *    <td>{@link #takeLast takeLast()}</td>
 *    <td>{@link #pollLast(long, TimeUnit) pollLast(time, unit)}</td>
 *  </tr>
 * </table>
 *
 * <p>Like any {@link BlockingQueue}, a <tt>BlockingDeque</tt> is
 * thread safe and may (or may not) be capacity-constrained.  A
 * <tt>BlockingDeque</tt> implementation may be used directly as a
 * FIFO <tt>BlockingQueue</tt>. The blocking methods inherited from
 * the <tt>BlockingQueue</tt> interface are precisely equivalent to
 * <tt>BlockingDeque</tt> methods as indicated in the following table:<p>
 *
 * <table BORDER CELLPADDING=3 CELLSPACING=1>
 *  <tr>
 *    <td ALIGN=CENTER> <b><tt>BlockingQueue</tt> Method</b></td>
 *    <td ALIGN=CENTER> <b>Equivalent <tt>BlockingDeque</tt> Method</b></td>
 *  </tr>
 *  <tr>
 *   <tr>
 *    <td>{@link java.util.concurrent.BlockingQueue#put put(e)}</td>
 *    <td>{@link #putLast putLast(e)}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link java.util.concurrent.BlockingQueue#take take()}</td>
 *    <td>{@link #takeFirst takeFirst()}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link java.util.concurrent.BlockingQueue#offer(Object, long, TimeUnit) offer(e, time. unit)}</td>
 *    <td>{@link #offerLast(Object, long, TimeUnit) offerLast(e, time, unit)}</td>
 *   </tr>
 *   <tr>
 *    <td>{@link java.util.concurrent.BlockingQueue#poll(long, TimeUnit) poll(time, unit)}</td>
 *    <td>{@link #pollFirst(long, TimeUnit) pollFirst(time, unit)}</td>
 *   </tr>
 * </table>
 *
 *
 * <p>This interface is a member of the
 * <a href="{@docRoot}/../guide/collections/index.html">
 * Java Collections Framework</a>.
 *
 * @since 1.6
 * @author Doug Lea
 * @param <E> the type of elements held in this collection
 */
public interface BlockingDeque<E> extends Deque<E>, BlockingQueue<E>
{

   /**
    * Adds the specified element as the first element of this deque,
    * waiting if necessary for space to become available.
    * @param o the element to add
    * @throws InterruptedException if interrupted while waiting.
    * @throws NullPointerException if the specified element is <tt>null</tt>.
    */
   void putFirst(E o) throws InterruptedException;

   /**
    * Adds the specified element as the last element of this deque,
    * waiting if necessary for space to become available.
    * @param o the element to add
    * @throws InterruptedException if interrupted while waiting.
    * @throws NullPointerException if the specified element is <tt>null</tt>.
    */
   void putLast(E o) throws InterruptedException;

   /**
    * Retrieves and removes the first element of this deque, waiting
    * if no elements are present on this deque.
    * @return the head of this deque
    * @throws InterruptedException if interrupted while waiting.
    */
   E takeFirst() throws InterruptedException;

   /**
    * Retrieves and removes the last element of this deque, waiting
    * if no elements are present on this deque.
    * @return the head of this deque
    * @throws InterruptedException if interrupted while waiting.
    */
   E takeLast() throws InterruptedException;

   /**
    * Inserts the specified element as the first element of this deque,
    * waiting if necessary up to the specified wait time for space to
    * become available.
    * @param o the element to add
    * @param timeout how long to wait before giving up, in units of
    * <tt>unit</tt>
    * @param unit a <tt>TimeUnit</tt> determining how to interpret the
    * <tt>timeout</tt> parameter
    * @return <tt>true</tt> if successful, or <tt>false</tt> if
    * the specified waiting time elapses before space is available.
    * @throws InterruptedException if interrupted while waiting.
    * @throws NullPointerException if the specified element is <tt>null</tt>.
    */
   boolean offerFirst(E o, long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * Inserts the specified element as the last element of this deque,
    * waiting if necessary up to the specified wait time for space to
    * become available.
    * @param o the element to add
    * @param timeout how long to wait before giving up, in units of
    * <tt>unit</tt>
    * @param unit a <tt>TimeUnit</tt> determining how to interpret the
    * <tt>timeout</tt> parameter
    * @return <tt>true</tt> if successful, or <tt>false</tt> if
    * the specified waiting time elapses before space is available.
    * @throws InterruptedException if interrupted while waiting.
    * @throws NullPointerException if the specified element is <tt>null</tt>.
    */
   boolean offerLast(E o, long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * Retrieves and removes the first element of this deque, waiting
    * if necessary up to the specified wait time if no elements are
    * present on this deque.
    * @param timeout how long to wait before giving up, in units of
    * <tt>unit</tt>
    * @param unit a <tt>TimeUnit</tt> determining how to interpret the
    * <tt>timeout</tt> parameter
    * @return the head of this deque, or <tt>null</tt> if the
    * specified waiting time elapses before an element is present.
    * @throws InterruptedException if interrupted while waiting.
    */
   E pollFirst(long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * Retrieves and removes the last element of this deque, waiting
    * if necessary up to the specified wait time if no elements are
    * present on this deque.
    * @param timeout how long to wait before giving up, in units of
    * <tt>unit</tt>
    * @param unit a <tt>TimeUnit</tt> determining how to interpret the
    * <tt>timeout</tt> parameter
    * @return the head of this deque, or <tt>null</tt> if the
    * specified waiting time elapses before an element is present.
    * @throws InterruptedException if interrupted while waiting.
    */
   E pollLast(long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * Adds the specified element as the last element of this deque,
    * waiting if necessary for space to become available.  This
    * method is equivalent to to putLast
    * @param o the element to add
    * @throws InterruptedException if interrupted while waiting.
    * @throws NullPointerException if the specified element is <tt>null</tt>.
    */
   void put(E o) throws InterruptedException;

   /** 
    * Inserts the specified element as the lest element of this
    * deque, if possible.  When using deques that may impose
    * insertion restrictions (for example capacity bounds), method
    * <tt>offer</tt> is generally preferable to method {@link
    * Collection#add}, which can fail to insert an element only by
    * throwing an exception.  This method is equivalent to to
    * offerLast
    *
    * @param o the element to add.
    * @return <tt>true</tt> if it was possible to add the element to
    *         this deque, else <tt>false</tt>
    * @throws NullPointerException if the specified element is <tt>null</tt>
    */
   boolean offer(E o, long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * Retrieves and removes the first element of this deque, waiting
    * if no elements are present on this deque.
    * This method is equivalent to to takeFirst 
    * @return the head of this deque
    * @throws InterruptedException if interrupted while waiting.
    */
   E take() throws InterruptedException;

   /**
    * Retrieves and removes the first element of this deque, waiting
    * if necessary up to the specified wait time if no elements are
    * present on this deque.  This method is equivalent to to
    * pollFirst
    * @param timeout how long to wait before giving up, in units of
    * <tt>unit</tt>
    * @param unit a <tt>TimeUnit</tt> determining how to interpret the
    * <tt>timeout</tt> parameter
    * @return the head of this deque, or <tt>null</tt> if the
    * specified waiting time elapses before an element is present.
    * @throws InterruptedException if interrupted while waiting.
    */
   E poll(long timeout, TimeUnit unit) throws InterruptedException;
}

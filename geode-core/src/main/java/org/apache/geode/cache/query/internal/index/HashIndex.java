/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.internal.index;

import static java.lang.System.lineSeparator;
import static org.apache.geode.cache.query.internal.CompiledValue.indexThresholdSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledIteratorDef;
import org.apache.geode.cache.query.internal.CompiledPath;
import org.apache.geode.cache.query.internal.CompiledSortCriterion;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.IndexInfo;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.QueryUtils;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.internal.Support;
import org.apache.geode.cache.query.internal.index.HashIndex.IMQEvaluator.HashIndexComparator;
import org.apache.geode.cache.query.internal.index.IndexStore.IndexStoreEntry;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A HashIndex is an index that can be used for equal and not equals queries It is created only when
 * called explicitly with createHashIndex It requires the indexed expression be a path expression
 * and the from clause has only one iterator. This implies there is only one value in the index for
 * each region entry.
 * <p>
 * This index does not support the storage of projection attributes.
 * <p>
 * Currently this implementation only supports an index on a region path.
 *
 * @deprecated Due to the overhead caused by rehashing while expanding the backing array, Hash Index
 *             has been deprecated since Apache Geode 1.4.0. Instead the use of
 *             {@link CompactRangeIndex} is recommended
 */
@Deprecated
public class HashIndex extends AbstractIndex {
  private static final Logger logger = LogService.getLogger();

  /**
   * ThreadLocal for Map for under update RegionEntries=>oldKey (reverse map) if
   * {@link IndexManager#INPLACE_OBJECT_MODIFICATION} is false.
   */
  protected ThreadLocal<Object2ObjectOpenHashMap> entryToOldKeysMap;

  /**
   * Map for valueOf(indexedExpression)=>RegionEntries. SortedMap<Object, (RegionEntry |
   * List<RegionEntry>)>. Package access for unit tests.
   */
  final HashIndexSet entriesSet;

  /**
   * Map for RegionEntries=>value of indexedExpression (reverse map) maintained by the HashIndexSet
   * entrieSet
   */
  private ConcurrentMap<Object, Object> entryToValuesMap = null;

  private boolean indexOnRegionKeys = false;

  private boolean indexOnValues = false;

  // used for sorting asc and desc queries
  private HashIndexComparator comparator;

  /**
   * Create a HashIndex that can be used when executing queries.
   *
   * @param indexName the name of this index, used for statistics collection
   * @param indexedExpression the expression to index on, a function dependent on region entries
   *        individually, limited to a path expression.
   * @param fromClause expression that evaluates to the collection(s) that will be queried over,
   *        must contain one and only one region path, and only one iterator.
   * @param projectionAttributes not used
   * @param definitions the canonicalized definitions
   */
  public HashIndex(InternalCache cache, String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes, String origFromClause,
      String origIndexExpr, String[] definitions, IndexStatistics stats) {
    super(cache, indexName, region, fromClause, indexedExpression, projectionAttributes,
        origFromClause, origIndexExpr, definitions, stats);
    RegionAttributes ra = region.getAttributes();


    if (IndexManager.isObjectModificationInplace()) {
      entryToValuesMap = new ConcurrentHashMap(ra.getInitialCapacity(), ra.getLoadFactor(),
          ra.getConcurrencyLevel());
    } else {
      if (entryToOldKeysMap == null) {
        entryToOldKeysMap = new ThreadLocal<>();
      }
    }

    entriesSet = new HashIndexSet();
  }

  /**
   * Get the index type
   *
   * @return the type of index
   */
  @Override
  public IndexType getType() {
    return IndexType.HASH;
  }

  @Override
  protected boolean isCompactRangeIndex() {
    return false;
  }

  @Override
  public void initializeIndex(boolean loadEntries) throws IMQException {
    long startTime = System.nanoTime();
    evaluator.initializeIndex(loadEntries);
    internalIndexStats.incNumUpdates(((IMQEvaluator) evaluator).getTotalEntriesUpdated());
    long endTime = System.nanoTime();
    internalIndexStats.incUpdateTime(endTime - startTime);
  }

  @Override
  void addMapping(RegionEntry entry) throws IMQException {
    evaluator.evaluate(entry, true);
    internalIndexStats.incNumUpdates();
  }

  /**
   * Add/Updates the index forward and reverse map. If index key for a RegionEntry is found same as
   * previous key no update is performed.
   *
   * This also updates the {@link IndexStatistics} numKeys and numValues as and when appropriate.
   * One thing to notice though is no increment in numValues is performed if old key and new index
   * key are found equal using {@link Object#equals(Object)}.
   */
  private void basicAddMapping(Object key, RegionEntry entry) throws IMQException {

    try {
      if (DefaultQuery.testHook != null) {
        DefaultQuery.testHook.doTestHook(
            DefaultQuery.TestHook.SPOTS.BEFORE_ADD_OR_UPDATE_MAPPING_OR_DESERIALIZING_NTH_STREAMINGOPERATION,
            null, null);
      }
      Object newKey = TypeUtils.indexKeyFor(key);
      if (newKey.equals(QueryService.UNDEFINED)) {
        Object targetObject = getTargetObjectForUpdate(entry);
        if (Token.isInvalidOrRemoved(targetObject)) {
          // This should not happen as a token should only be added during gii
          // meaning we do not have an old mapping
          // we will continue to remove the old mapping to be safe and log a fine level message
          Object oldKey = null;
          if (IndexManager.isObjectModificationInplace()
              && entryToValuesMap.containsKey(entry)) {
            oldKey = entryToValuesMap.get(entry);
          } else if (!IndexManager.isObjectModificationInplace()
              && entryToOldKeysMap != null) {
            Map oldKeyMap = entryToOldKeysMap.get();
            if (oldKeyMap != null) {
              oldKey = TypeUtils.indexKeyFor(oldKeyMap.get(entry));
            }
          }
          if (oldKey != null) {
            if (logger.isDebugEnabled()) {
              logger
                  .debug("A removed or invalid token was being added, and we had an old mapping.");
            }
            removeFromEntriesSet(oldKey, entry, true);
          }
          return;
        }
      }

      // Before adding the entry with new value, remove it from reverse map and
      // using the oldValue remove entry from the forward map.
      // Reverse-map is used based on the system property
      Object oldKey = getOldKey(entry);

      int indexSlot = entriesSet.add(newKey, entry);

      if (indexSlot >= 0) {
        // Update the reverse map
        if (IndexManager.isObjectModificationInplace()) {
          entryToValuesMap.put(entry, newKey);
        }
        if (newKey != null && oldKey != null) {
          removeFromEntriesSet(oldKey, entry, false, indexSlot);
        }
        // Update Stats after real addition
        internalIndexStats.incNumValues(1);

      }
    } catch (TypeMismatchException ex) {
      throw new IMQException("Could not add object of type " + key.getClass().getName(), ex);
    }
  }

  private Object getOldKey(RegionEntry entry) throws TypeMismatchException {
    Object oldKey = null;
    if (IndexManager.isObjectModificationInplace() && entryToValuesMap.containsKey(entry)) {
      oldKey = entryToValuesMap.get(entry);
    } else if (!IndexManager.isObjectModificationInplace() && entryToOldKeysMap != null) {
      Map oldKeyMap = entryToOldKeysMap.get();
      if (oldKeyMap != null) {
        oldKey = TypeUtils.indexKeyFor(oldKeyMap.get(entry));
      }
    }
    return oldKey;
  }

  /**
   * @param opCode one of OTHER_OP, BEFORE_UPDATE_OP, AFTER_UPDATE_OP.
   */
  @Override
  void removeMapping(RegionEntry entry, int opCode) throws IMQException {
    // logger.debug("##### In RemoveMapping: entry : "
    // + entry );
    if (opCode == BEFORE_UPDATE_OP) {
      // Either take key from reverse map OR evaluate it using IMQEvaluator.
      if (!IndexManager.isObjectModificationInplace()) {
        // It will always contain 1 element only, for this thread.
        entryToOldKeysMap.set(new Object2ObjectOpenHashMap(1));
        evaluator.evaluate(entry, false);
      }
    } else if (opCode == REMOVE_DUE_TO_GII_TOMBSTONE_CLEANUP) {
      // we know in this specific case, that a before op was called and stored oldKey/value
      // we also know that a regular remove won't work due to the entry no longer being present
      // We know the old key so let's just remove mapping from the old key
      if (entryToOldKeysMap != null) {
        basicRemoveMapping(entryToOldKeysMap.get().get(entry), entry, true);
      }
    } else if (opCode == CLEAN_UP_THREAD_LOCALS) {
      if (entryToOldKeysMap != null) {
        entryToOldKeysMap.remove();
      }
    } else {
      // Need to reset the thread-local map as many puts and destroys might
      // happen in same thread.
      if (entryToOldKeysMap != null) {
        entryToOldKeysMap.remove();
      }
      evaluator.evaluate(entry, false);
      internalIndexStats.incNumUpdates();
    }
  }

  /**
   * Remove an index entry for a RegionEntry when invalidate/destroy is called OR new index key is
   * inserted for the RegionEntry. In case of update only forward map is cleared of old key and NO
   * update is performed on reverse map as that has already been done during
   * {@link HashIndex#basicAddMapping(Object, RegionEntry)}.
   *
   * @param key - Index key.
   * @param entry RegionEntry for which is being updated by user.
   * @param updateReverseMap true only when RegionEntry is invalidated/destroyed.
   */
  private void basicRemoveMapping(Object key, RegionEntry entry, boolean updateReverseMap)
      throws IMQException {
    // after removal, trim the ArrayList to prevent
    // too much extra space.
    // Ideally we would only trim if there is excessive
    // space, but there is no way to ask an ArrayList what
    // it's current capacity is..so we trim after every
    // removal
    try {
      Object newKey = TypeUtils.indexKeyFor(key);
      removeFromEntriesSet(newKey, entry, updateReverseMap);
    } catch (TypeMismatchException ex) {
      throw new IMQException("Could not add object of type " + key.getClass().getName(), ex);
    }
  }

  private void removeFromEntriesSet(Object newKey, RegionEntry entry, boolean updateReverseMap) {
    removeFromEntriesSet(newKey, entry, updateReverseMap, -1);
  }

  private void removeFromEntriesSet(Object newKey, RegionEntry entry, boolean updateReverseMap,
      int ignoreThisSlot) {
    if (entriesSet.remove(newKey, entry, ignoreThisSlot)) {
      if (updateReverseMap && IndexManager.isObjectModificationInplace()) {
        entryToValuesMap.remove(entry);
      }
      internalIndexStats.incNumValues(-1);
    }
  }

  // // IndexProtocol interface implementation
  @Override
  public boolean clear() throws QueryException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * computes the resultset of an equijoin query
   */
  @Override
  public List queryEquijoinCondition(IndexProtocol indx, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    // get a read lock when doing a lookup
    long start = updateIndexUseStats();
    ((AbstractIndex) indx).updateIndexUseStats();
    List data = new ArrayList();
    Iterator inner = null;
    try {
      // We will iterate over each of the valueToEntries Map to obtain the keys
      Iterator outer = entriesSet.iterator();
      if (indx instanceof CompactRangeIndex) {
        inner = ((CompactRangeIndex) indx).getIndexStorage().iterator(null);
      } else {
        inner = ((RangeIndex) indx).getValueToEntriesMap().entrySet().iterator();
      }
      Map.Entry outerEntry = null;
      Object innerEntry = null;
      Object outerKey = null;
      Object innerKey = null;
      // boolean incrementOuter = true;
      boolean incrementInner = true;
      outer: while (outer.hasNext()) {
        // if (incrementOuter) {
        outerEntry = (Map.Entry) outer.next();
        // }
        outerKey = outerEntry.getKey();
        // TODO: eliminate use of labels
        inner: while (!incrementInner || inner.hasNext()) {
          if (incrementInner) {
            innerEntry = inner.next();
            if (innerEntry instanceof IndexStoreEntry) {
              innerKey = ((IndexStoreEntry) innerEntry).getDeserializedKey();
            } else {
              innerKey = ((Map.Entry) innerEntry).getKey();
            }
          }
          int compare = ((Comparable) outerKey).compareTo(innerKey);
          if (compare == 0) {
            Object innerValue = null;
            if (innerEntry instanceof IndexStoreEntry) {
              innerValue = ((CompactRangeIndex) indx).getIndexStorage().get(outerKey);
            } else {
              innerValue = ((Map.Entry) innerEntry).getValue();
            }
            populateListForEquiJoin(data, outerEntry.getValue(), innerValue, context, innerKey);

            incrementInner = true;
            continue outer;
          } else if (compare < 0) {
            // Asif :The outer key is smaller than the inner key. That means
            // that we need
            // to increment the outer loop without moving inner loop.
            // incrementOuter = true;
            incrementInner = false;
            continue outer;
          } else {
            // The outer key is greater than inner key , so increment the
            // inner loop without changing outer
            incrementInner = true;
          }
        }
        break;
      }
      return data;
    } finally {
      ((AbstractIndex) indx).updateIndexUseEndStats(start);
      updateIndexUseEndStats(start);
      if (inner != null && indx instanceof CompactRangeIndex) {
        ((CloseableIterator<IndexStoreEntry>) inner).close();
      }
    }
  }

  /**
   * This evaluates the left and right side of a EQUI-JOIN where condition for which this Index was
   * used. Like, if condition is "p.ID = e.ID", {@link IndexInfo} will contain Left as p.ID, Right
   * as e.ID and operator as TOK_EQ. This method will evaluate p.ID OR e.ID based on if it is inner
   * or outer RegionEntry, and verify the p.ID = e.ID.
   *
   * @return true if entry value and index value are consistent.
   */
  private boolean verifyInnerAndOuterEntryValues(RegionEntry entry, ExecutionContext context,
      IndexInfo indexInfo, Object keyVal) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // Verify index key in value.
    CompactRangeIndex index = (CompactRangeIndex) indexInfo._getIndex();
    RuntimeIterator runtimeItr = index.getRuntimeIteratorForThisIndex(context, indexInfo);
    if (runtimeItr != null) {
      runtimeItr.setCurrent(getTargetObject(entry));
    }
    return evaluateEntry(indexInfo, context, keyVal);
  }

  @Override
  public int getSizeEstimate(Object key, int operator, int matchLevel)
      throws TypeMismatchException {
    // Get approx size;
    int size = 0;
    long start = updateIndexUseStats(false);
    try {
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          key = TypeUtils.indexKeyFor(key);
          size = entriesSet.size(key);
        }
          break;
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE:
          size = region.size();
          key = TypeUtils.indexKeyFor(key);
          size = entriesSet.size(key);
          break;
      }
    } finally {
      updateIndexUseEndStats(start, false);
    }
    return size;
  }

  /**
   * Convert a RegionEntry or THashSet<RegionEntry> to be consistently a Collection
   */
  private Collection regionEntryCollection(Object regionEntries) {
    if (regionEntries == null) {
      return null;
    }
    if (regionEntries instanceof RegionEntry) {
      return Collections.singleton(regionEntries);
    }
    return (Collection) regionEntries;
  }

  /** Method called while appropriate lock held on index */
  private void lockedQueryPrivate(Object key, int operator, Collection results,
      CompiledValue iterOps, RuntimeIterator runtimeItr, ExecutionContext context, Set keysToRemove,
      List projAttrib, SelectResults intermediateResults, boolean isIntersection)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    if (keysToRemove == null) {
      keysToRemove = new HashSet(0);
    }
    int limit = -1;

    Boolean applyLimit = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX);
    if (applyLimit != null && applyLimit) {
      limit = (Integer) context.cacheGet(CompiledValue.RESULT_LIMIT);
      if (limit != -1 && limit < indexThresholdSize) {
        limit = indexThresholdSize;
      }
    }

    Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
    boolean applyOrderBy = false;
    List orderByAttrs = null;
    if (orderByClause != null && orderByClause) {
      orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttrs.get(0);
      applyOrderBy = true;
    }
    evaluate(key, operator, results, iterOps, runtimeItr, context, keysToRemove, projAttrib,
        intermediateResults, isIntersection, limit, applyOrderBy, orderByAttrs);
  }

  /** Method called while appropriate lock held on index */
  @Override
  void lockedQuery(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    throw new UnsupportedOperationException(
        "Range grouping for HashIndex condition is not supported");

  }

  private void evaluate(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, Set keysToRemove, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection, int limit, boolean applyOrderBy,
      List orderByAttribs) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    boolean multiColOrderBy = false;
    if (keysToRemove == null) {
      keysToRemove = new HashSet(0);
    }
    key = TypeUtils.indexKeyFor(key);
    if (key == null) {
      key = IndexManager.NULL;
    }
    boolean asc = true;
    if (applyOrderBy) {
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttribs.get(0);
      asc = !csc.getCriterion();
      multiColOrderBy = orderByAttribs.size() > 1;
    }

    try {
      long iteratorCreationTime = cache.cacheTimeMillis();

      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ:
          assert keysToRemove.isEmpty();
          addToResultsFromEntries(entriesSet.get(key), results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit,
              keysToRemove, applyOrderBy, asc, iteratorCreationTime);
          break;
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE: {
          keysToRemove.add(key);
          addToResultsFromEntries(entriesSet.getAllNotMatching(keysToRemove), results, iterOps,
              runtimeItr, context, projAttrib, intermediateResults, isIntersection,
              multiColOrderBy ? -1 : limit, keysToRemove, applyOrderBy, asc, iteratorCreationTime);
        }
          break;
        default:
          throw new AssertionError("Operator = " + operator);
      } // end switch
    } catch (ClassCastException ex) {
      if (operator == OQLLexerTokenTypes.TOK_EQ) { // result is empty
        // set
        return;
      } else if (operator == OQLLexerTokenTypes.TOK_NE
          || operator == OQLLexerTokenTypes.TOK_NE_ALT) { // put
        keysToRemove.add(key);
        long iteratorCreationTime = cache.cacheTimeMillis();
        addToResultsFromEntries(entriesSet.getAllNotMatching(keysToRemove), results, iterOps,
            runtimeItr, context, projAttrib, intermediateResults, isIntersection,
            multiColOrderBy ? -1 : limit, keysToRemove, applyOrderBy, asc, iteratorCreationTime);
      } else { // otherwise throw exception
        throw new TypeMismatchException("", ex);
      }
    }
  }

  @Override
  void instantiateEvaluator(IndexCreationHelper indexCreationHelper) {
    evaluator = new IMQEvaluator(indexCreationHelper);
    entriesSet.setEvaluator((HashIndex.IMQEvaluator) evaluator);
    comparator = ((IMQEvaluator) evaluator).comparator;
  }

  @Override
  public ObjectType getResultSetType() {
    return evaluator.getIndexResultSetType();
  }


  /**
   * @param entriesIter is Iterable<RegionEntry>
   */
  private void addToResultsFromEntries(Iterator entriesIter, Collection result,
      CompiledValue iterOps, RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection, int limit, Set keysToRemove,
      boolean applyOrderBy, boolean asc, long iteratorCreationTime) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    QueryObserver observer = QueryObserverHolder.getInstance();
    if (result == null || limit != -1 && result.size() == limit) {
      return;
    }
    List orderedKeys = null;
    List orderedResults = null;
    if (applyOrderBy) {
      orderedKeys = new ArrayList();
      orderedResults = new ArrayList();
    }
    int i = 0;
    while (entriesIter.hasNext()) {
      // Check if query execution on this thread is canceled.
      QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCanceled();
      if (IndexManager.testHook != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("IndexManager TestHook is set in addToResultsFromEntries.");
        }
        IndexManager.testHook.hook(11);
      }
      Object obj = entriesIter.next();
      Object key = null;
      if (obj != null && obj != HashIndexSet.REMOVED) {
        RegionEntry re = (RegionEntry) obj;
        if (applyOrderBy) {
          key = ((HashIndex.IMQEvaluator) evaluator).evaluateKey(obj);
          orderedKeys.add(new Object[] {key, i++});
          addValueToResultSet(re, orderedResults, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit, observer, iteratorCreationTime);
        } else {
          addValueToResultSet(re, result, iterOps, runtimeItr, context, projAttrib,
              intermediateResults, isIntersection, limit, observer, iteratorCreationTime);
        }
      }
    }
    if (applyOrderBy) {
      /*
       * For orderby queries, 1. Store the keys in a list along with the order. 2. Store the results
       * in another temp list. 3. Sort the keys. The order will also get sorted. 4. Fetch the result
       * objects from the temp list according to the sorted orders from the sorted list and add to
       * the result collection.
       */
      Collections.sort(orderedKeys, comparator);
      if (!asc) {
        Collections.reverse(orderedKeys);
      }
      Object[] temp = orderedResults.toArray();
      List tempResults = new ArrayList(temp.length);
      for (Object o : orderedKeys) {
        int index = (Integer) ((Object[]) o)[1];
        tempResults.add(temp[index]);
      }
      result.addAll(tempResults);
    }
  }

  private void addValueToResultSet(RegionEntry re, Collection result, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection, int limit, QueryObserver observer,
      long iteratorCreationTime) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    Object value = getTargetObject(re);
    if (value != null) {
      boolean ok = true;
      // If the region entry is currently being updated or it has been modified since starting
      // iteration
      // we will reevaluate to be sure the value still matches the key
      if (re.isUpdateInProgress()
          || IndexManager.needsRecalculation(iteratorCreationTime, re.getLastModified())) {
        IndexInfo indexInfo = (IndexInfo) context.cacheGet(CompiledValue.INDEX_INFO);
        if (runtimeItr == null) {
          runtimeItr = getRuntimeIteratorForThisIndex(context, indexInfo);
          if (runtimeItr == null) {
            // could not match index with iterator
            throw new QueryInvocationTargetException("Query alias's must be used consistently");
          }
        }
        runtimeItr.setCurrent(value);
        // Verify index key in region entry value.
        ok = evaluateEntry(indexInfo, context, null);
      }
      if (runtimeItr != null) {
        runtimeItr.setCurrent(value);
      }
      if (ok && runtimeItr != null && iterOps != null) {
        ok = QueryUtils.applyCondition(iterOps, context);
      }
      if (ok) {
        applyCqOrProjection(projAttrib, context, result, value, intermediateResults,
            isIntersection, re.getKey());
        if (limit != -1 && result.size() == limit) {
          observer.limitAppliedAtIndexLevel(this, limit, result);
          return;
        }
      }
    }
  }

  /**
   * This evaluates the left and right side of a where condition for which this Index was used.
   * Like, if condition is "ID > 1", {@link IndexInfo} will contain Left as ID, Right as '1' and
   * operator as TOK_GT. This method will evaluate ID from region entry value and verify the ID > 1.
   *
   * Note: IndexInfo is created for each query separately based on the condition being evaluated
   * using the Index.
   *
   * @return true if RegionEntry value satisfies the where condition (contained in IndexInfo).
   */
  private boolean evaluateEntry(IndexInfo indexInfo, ExecutionContext context, Object keyVal)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    CompiledValue path = indexInfo._path();
    Object left = path.evaluate(context);
    CompiledValue key = indexInfo._key();
    Object right = null;

    // For CompiledUndefined indexInfo has null key.
    if (keyVal == null && key == null) {
      return left == QueryService.UNDEFINED;
    }

    if (key != null) {
      right = key.evaluate(context);
    } else {
      right = keyVal;
    }

    int operator = indexInfo._operator();
    if (left == null && right == null) {
      return Boolean.TRUE;
    } else {
      return (Boolean) TypeUtils.compare(left, right, operator);
    }
  }

  /**
   * Get the object of interest from the region entry. For now it always gets the deserialized
   * value.
   */
  private Object getTargetObject(RegionEntry entry) {
    if (indexOnValues) {
      // OFFHEAP: incrc, deserialize, decrc
      Object o = entry.getValue((LocalRegion) getRegion());
      try {
        if (o == Token.INVALID) {
          return null;
        }
        if (o instanceof CachedDeserializable) {
          return ((CachedDeserializable) o).getDeserializedForReading();
        }
      } catch (EntryDestroyedException ignored) {
        return null;
      }
      return o;
    } else if (indexOnRegionKeys) {
      return entry.getKey();
    }
    return new NonTXEntry((LocalRegion) getRegion(), entry);
  }

  private Object getTargetObjectForUpdate(RegionEntry entry) {
    if (indexOnValues) {
      Object o = entry.getValueOffHeapOrDiskWithoutFaultIn((LocalRegion) getRegion());
      try {
        if (o instanceof StoredObject) {
          StoredObject ohval = (StoredObject) o;
          try {
            o = ohval.getDeserializedForReading();
          } finally {
            ohval.release();
          }
        } else if (o instanceof CachedDeserializable) {
          o = ((CachedDeserializable) o).getDeserializedForReading();
        }
      } catch (EntryDestroyedException ignored) {
        return Token.INVALID;
      }
      return o;
    } else if (indexOnRegionKeys) {
      return entry.getKey();
    }
    return new NonTXEntry((LocalRegion) getRegion(), entry);
  }

  @Override
  void recreateIndexData() throws IMQException {
    // Mark the data maps to null & call the initialization code of index
    entriesSet.clear();
    if (IndexManager.isObjectModificationInplace()) {
      entryToValuesMap.clear();
    }
    int numKeys = (int) internalIndexStats.getNumberOfKeys();
    if (numKeys > 0) {
      internalIndexStats.incNumKeys(-numKeys);
    }
    int numValues = (int) internalIndexStats.getNumberOfValues();
    if (numValues > 0) {
      internalIndexStats.incNumValues(-numValues);
    }
    int updates = (int) internalIndexStats.getNumUpdates();
    if (updates > 0) {
      internalIndexStats.incNumUpdates(updates);
    }
    initializeIndex(true);
  }

  public String dump() {
    StringBuilder sb = new StringBuilder(toString()).append(" {").append(lineSeparator());
    sb.append(" -----------------------------------------------").append(lineSeparator());
    for (Object anEntriesSet : entriesSet) {
      Entry indexEntry = (Entry) anEntriesSet;
      sb.append(" Key = ").append(indexEntry.getKey()).append(lineSeparator());
      sb.append(" Value Type = ").append(' ').append(indexEntry.getValue().getClass().getName())
          .append(lineSeparator());
      if (indexEntry.getValue() instanceof Collection) {
        sb.append(" Value Size = ").append(' ').append(((Collection) indexEntry.getValue()).size())
            .append(lineSeparator());
      } else if (indexEntry.getValue() instanceof RegionEntry) {
        sb.append(" Value Size = ").append(" " + 1).append(lineSeparator());
      } else {
        throw new AssertionError("value instance of " + indexEntry.getValue().getClass().getName());
      }
      Collection entrySet = regionEntryCollection(indexEntry.getValue());
      for (Object anEntrySet : entrySet) {
        RegionEntry e = (RegionEntry) anEntrySet;
        Object value = getTargetObject(e);
        sb.append("  RegionEntry.key = ").append(e.getKey());
        sb.append("  Value.type = ").append(value.getClass().getName());
        if (value instanceof Collection) {
          sb.append("  Value.size = ").append(((Collection) value).size());
        }
        sb.append(lineSeparator());
      }
      sb.append(" -----------------------------------------------").append(lineSeparator());
    }
    sb.append("}// Index ").append(getName()).append(" end");
    return sb.toString();
  }

  @Override
  protected InternalIndexStatistics createStats(String indexName) {
    return new RangeIndexStatistics(indexName);
  }

  class RangeIndexStatistics extends InternalIndexStatistics {
    private final IndexStats vsdStats;

    public RangeIndexStatistics(String indexName) {
      vsdStats = new IndexStats(getRegion().getCache().getDistributedSystem(), indexName);
    }

    /**
     * Return the total number of times this index has been updated
     */
    @Override
    public long getNumUpdates() {
      return vsdStats.getNumUpdates();
    }

    @Override
    public void incNumValues(int delta) {
      vsdStats.incNumValues(delta);
    }

    @Override
    public void incNumUpdates() {
      vsdStats.incNumUpdates();
    }

    @Override
    public void incNumUpdates(int delta) {
      vsdStats.incNumUpdates(delta);
    }

    @Override
    public void updateNumKeys(long numKeys) {
      vsdStats.updateNumKeys(numKeys);
    }

    @Override
    public void incNumKeys(long numKeys) {
      vsdStats.incNumKeys(numKeys);
    }

    @Override
    public void incUpdateTime(long delta) {
      vsdStats.incUpdateTime(delta);
    }

    @Override
    public void incUpdatesInProgress(int delta) {
      vsdStats.incUpdatesInProgress(delta);
    }

    @Override
    public void incNumUses() {
      vsdStats.incNumUses();
    }

    @Override
    public void incUseTime(long delta) {
      vsdStats.incUseTime(delta);
    }

    @Override
    public void incUsesInProgress(int delta) {
      vsdStats.incUsesInProgress(delta);
    }

    @Override
    public void incReadLockCount(int delta) {
      vsdStats.incReadLockCount(delta);
    }

    /**
     * Returns the total amount of time (in nanoseconds) spent updating this index.
     */
    @Override
    public long getTotalUpdateTime() {
      return vsdStats.getTotalUpdateTime();
    }

    /**
     * Returns the total number of times this index has been accessed by a query.
     */
    @Override
    public long getTotalUses() {
      return vsdStats.getTotalUses();
    }

    /**
     * Returns the number of keys in this index.
     */
    @Override
    public long getNumberOfKeys() {
      return vsdStats.getNumberOfKeys();
    }

    /**
     * Returns the number of values in this index.
     */
    @Override
    public long getNumberOfValues() {
      return vsdStats.getNumberOfValues();
    }

    /**
     * Return the number of values for the specified key in this index.
     */
    @Override
    public long getNumberOfValues(Object key) {
      Object rgnEntries = entriesSet.get(key);
      if (rgnEntries == null) {
        return 0;
      }
      if (rgnEntries instanceof RegionEntry) {
        return 1;
      } else {
        return ((Collection) rgnEntries).size();
      }
    }

    /**
     * Return the number of read locks taken on this index
     */
    @Override
    public int getReadLockCount() {
      return vsdStats.getReadLockCount();
    }

    @Override
    public void close() {
      vsdStats.close();
    }

    public String toString() {
      return "No Keys = " + getNumberOfKeys() + lineSeparator()
          + "No Values = " + getNumberOfValues() + lineSeparator()
          + "No Uses = " + getTotalUses() + lineSeparator()
          + "No Updates = " + getNumUpdates() + lineSeparator()
          + "Total Update time = " + getTotalUpdateTime() + lineSeparator();
    }
  }

  class IMQEvaluator implements IndexedExpressionEvaluator {
    private final InternalCache cache;
    private List fromIterators = null;
    private CompiledValue indexedExpr = null;
    private final String[] canonicalIterNames;
    private ObjectType indexResultSetType = null;
    private Region rgn = null;
    private Map dependencyGraph = null;
    final HashIndexComparator comparator = new HashIndexComparator();

    /*
     * The boolean if true indicates that the 0th iterator is on entries . If the 0th iterator is on
     * collection of Region.Entry objects, then the RegionEntry object used in Index data objects is
     * obtained directly from its corresponding Region.Entry object. However if the 0th iterator is
     * not on entries then the boolean is false. In this case the additional projection attribute
     * gives us the original value of the iterator while the Region.Entry object is obtained from
     * 0th iterator. It is possible to have index being created on a Region Entry itself , instead
     * of a Region. A Map operator( Compiled Index Operator) used with Region enables, us to create
     * such indexes. In such case the 0th iterator, even if it represents a collection of Objects
     * which are not Region.Entry objects, still the boolean remains true, as the Entry object can
     * be easily obtained from the 0th iterator. In this case, the additional projection attribute s
     * not null as it is used to evaluate the Entry object from the 0th iterator.
     */
    private boolean isFirstItrOnEntry = false;
    // Asif: List of modified iterators, not null only when the boolean
    // isFirstItrOnEntry is false.
    private List indexInitIterators = null;
    // The additional Projection attribute representing the value of the
    // original 0th iterator. If the isFirstItrOnEntry is false, then it is not
    // null. However if the isFirstItrOnEntry is true and this attribute is not
    // null, this indicates that the 0th iterator is derived using an individual
    // entry thru Map operator on the Region.
    private CompiledValue additionalProj = null;
    // This is not null iff the boolean isFirstItrOnEntry is false.
    private CompiledValue modifiedIndexExpr = null;
    private ObjectType addnlProjType = null;
    private int initEntriesUpdated = 0;
    private boolean hasInitOccurredOnce = false;
    private final boolean hasIndxUpdateOccurredOnce = false;
    private ExecutionContext initContext = null;
    private int iteratorSize = -1;

    /** Creates a new instance of IMQEvaluator */
    IMQEvaluator(IndexCreationHelper helper) {
      cache = helper.getCache();
      fromIterators = helper.getIterators();
      indexedExpr = helper.getCompiledIndexedExpression();
      canonicalIterNames = helper.canonicalizedIteratorNames;
      rgn = helper.getRegion();

      // The modified iterators for optimizing Index creation
      isFirstItrOnEntry = ((FunctionalIndexCreationHelper) helper).isFirstIteratorRegionEntry;
      additionalProj = ((FunctionalIndexCreationHelper) helper).additionalProj;
      Object[] params1 = {new QRegion(rgn, false)};
      initContext = new ExecutionContext(params1, cache);
      if (isFirstItrOnEntry) {
        indexInitIterators = fromIterators;
      } else {
        indexInitIterators = ((FunctionalIndexCreationHelper) helper).indexInitIterators;
        modifiedIndexExpr = ((FunctionalIndexCreationHelper) helper).modifiedIndexExpr;
        addnlProjType = ((FunctionalIndexCreationHelper) helper).addnlProjType;
      }
      iteratorSize = indexInitIterators.size();
      if (additionalProj instanceof CompiledPath) {
        String tailId = ((CompiledPath) additionalProj).getTailID();
        if (tailId.equals("key")) {
          // index on keys
          indexOnRegionKeys = true;
        } else if (!isFirstItrOnEntry) {
          // its not entries, its on value.
          indexOnValues = true;
        }
      }
    }

    @Override
    public String getIndexedExpression() {
      return getCanonicalizedIndexedExpression();
    }

    @Override
    public String getProjectionAttributes() {
      return getCanonicalizedProjectionAttributes();
    }

    @Override
    public String getFromClause() {
      return getCanonicalizedFromClause();
    }

    @Override
    public void expansion(List expandedResults, Object lowerBoundKey, Object upperBoundKey,
        int lowerBoundOperator, int upperBoundOperator, Object value) throws IMQException {
      // no-op
    }

    /**
     * @param add true if adding to index, false if removing
     */
    @Override
    public void evaluate(RegionEntry target, boolean add) throws IMQException {
      assert !target.isInvalid() : "value in RegionEntry should not be INVALID";
      ExecutionContext context = null;
      try {
        context = createExecutionContext(target);
        doNestedIterations(0, add, context);

      } catch (TypeMismatchException tme) {
        if (tme.getRootCause() instanceof EntryDestroyedException) {
          // This code relies on current implementation of remove mapping, relying on behavior that
          // will force a
          // crawl through the index to remove the entry if it exists, even if it is not present at
          // the provided key
          entriesSet.remove(QueryService.UNDEFINED, target, -1);
        } else {
          throw new IMQException(tme);
        }
      } catch (IMQException imqe) {
        throw imqe;
      } catch (Exception e) {
        throw new IMQException(e);
      } finally {
        if (context != null) {
          context.popScope();
        }
      }
    }

    /**
     * This function is used for creating Index data at the start
     *
     */
    @Override
    public void initializeIndex(boolean loadEntries) throws IMQException {
      initEntriesUpdated = 0;
      try {
        // Asif: Since an index initialization can happen multiple times
        // for a given region, due to clear operation, we are using harcoded
        // scope ID of 1 , as otherwise if obtained from ExecutionContext
        // object, it will get incremented on very index initialization
        initContext.newScope(1);
        for (int i = 0; i < iteratorSize; i++) {
          CompiledIteratorDef iterDef = (CompiledIteratorDef) indexInitIterators.get(i);
          RuntimeIterator rIter = null;
          if (!hasInitOccurredOnce) {
            iterDef.computeDependencies(initContext);
            rIter = iterDef.getRuntimeIterator(initContext);
            initContext.addToIndependentRuntimeItrMapForIndexCreation(iterDef);
          }
          if (rIter == null) {
            rIter = iterDef.getRuntimeIterator(initContext);
          }
          initContext.bindIterator(rIter);
        }
        hasInitOccurredOnce = true;
        if (indexResultSetType == null) {
          indexResultSetType = createIndexResultSetType();
        }
        if (loadEntries) {
          doNestedIterationsForIndexInit(0, initContext.getCurrentIterators());
        }
      } catch (IMQException imqe) {
        throw imqe;
      } catch (Exception e) {
        throw new IMQException(e);
      } finally {
        initContext.popScope();
      }
    }

    private void doNestedIterationsForIndexInit(int level, List runtimeIterators)
        throws TypeMismatchException, FunctionDomainException,
        NameResolutionException, QueryInvocationTargetException, IMQException {
      if (level == 1) {
        ++initEntriesUpdated;
      }
      if (level == iteratorSize) {
        applyProjectionForIndexInit(runtimeIterators);
      } else {
        RuntimeIterator rIter = (RuntimeIterator) runtimeIterators.get(level);
        // System.out.println("Level = "+level+" Iter = "+rIter.getDef());
        Collection c = rIter.evaluateCollection(initContext);
        if (c == null) {
          return;
        }
        for (final Object o : c) {
          rIter.setCurrent(o);
          doNestedIterationsForIndexInit(level + 1, runtimeIterators);
        }
      }
    }

    /*
     * This function is used to obtain Indxe data at the time of index creation. Each element of the
     * List is an Object Array of size 3. The 0th element of Object Array stores the value of Index
     * Expression. The 1st element of ObjectArray contains the RegionEntry object ( If the booelan
     * isFirstItrOnEntry is false, then the 0th iterator will give us the Region.Entry object which
     * can be used to obtain the underlying RegionEntry object. If the boolean is true & additional
     * projection attribute is not null, then the Region.Entry object can be obtained by evaluating
     * the additional projection attribute. If the boolean isFirstItrOnEntry is tru e& additional
     * projection attribute is null, then the 0th iterator itself will evaluate to Region.Entry
     * Object.
     *
     * The 2nd element of Object Array contains the Struct object ( tuple) created. If the boolean
     * isFirstItrOnEntry is false, then the first attribute of the Struct object is obtained by
     * evaluating the additional projection attribute.
     */
    private void applyProjectionForIndexInit(List currentRuntimeIters)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException, IMQException {
      if (QueryMonitor.isLowMemory()) {
        throw new IMQException(
            "Index creation canceled due to low memory");
      }

      Object indexKey = null;
      RegionEntry re = null;
      indexKey = isFirstItrOnEntry ? indexedExpr.evaluate(initContext)
          : modifiedIndexExpr.evaluate(initContext);
      if (indexKey == null) {
        indexKey = IndexManager.NULL;
      }
      NonTXEntry temp = null;
      if (isFirstItrOnEntry && additionalProj != null) {
        temp = (NonTXEntry) additionalProj.evaluate(initContext);
      } else {
        temp = (NonTXEntry) (((RuntimeIterator) currentRuntimeIters.get(0))
            .evaluate(initContext));
      }
      re = temp.getRegionEntry();
      basicAddMapping(indexKey, re);

    }

    /**
     * @param add true if adding to index, false if removing
     */
    private void doNestedIterations(int level, boolean add, ExecutionContext context)
        throws TypeMismatchException, FunctionDomainException,
        NameResolutionException, QueryInvocationTargetException, IMQException {
      List iterList = context.getCurrentIterators();
      if (level == iteratorSize) {
        applyProjection(add, context);
      } else {
        RuntimeIterator rIter = (RuntimeIterator) iterList.get(level);
        // System.out.println("Level = "+level+" Iter = "+rIter.getDef());
        Collection c = rIter.evaluateCollection(context);
        if (c == null) {
          return;
        }
        for (final Object o : c) {
          rIter.setCurrent(o);
          doNestedIterations(level + 1, add, context);
        }
      }
    }

    /**
     * @param add true if adding, false if removing from index
     */
    private void applyProjection(boolean add, ExecutionContext context)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException, IMQException {
      Object indexKey = indexedExpr.evaluate(context);
      if (indexKey == null) {
        indexKey = IndexManager.NULL;
      }

      RegionEntry entry = ((DummyQRegion) context.getBindArgument(1)).getEntry();
      // Get thread local reverse map if available.
      if (add) {
        // Add new index entries before removing old ones.
        basicAddMapping(indexKey, entry);
        if (entryToOldKeysMap != null) {
          entryToOldKeysMap.remove();
        }
      } else {
        if (entryToOldKeysMap != null) {
          Map oldKeyMap = entryToOldKeysMap.get();
          if (oldKeyMap != null) {
            oldKeyMap.put(entry, indexKey);
          } else {
            basicRemoveMapping(indexKey, entry, true);
          }
        } else {
          basicRemoveMapping(indexKey, entry, true);
        }
      }
    }

    // The struct type calculation is modified if the
    // 0th iterator is modified to make it dependent on Entry
    private ObjectType createIndexResultSetType() {
      List currentIterators = initContext.getCurrentIterators();
      int len = currentIterators.size();
      ObjectType type = null;
      // String fieldNames[] = new String[len];
      ObjectType[] fieldTypes = new ObjectType[len];
      int start = isFirstItrOnEntry ? 0 : 1;
      for (; start < len; start++) {
        RuntimeIterator iter = (RuntimeIterator) currentIterators.get(start);
        // fieldNames[start] = iter.getInternalId();
        fieldTypes[start] = iter.getElementType();
      }
      if (!isFirstItrOnEntry) {
        // fieldNames[0] = "iter1";
        fieldTypes[0] = addnlProjType;
      }
      type = (len == 1) ? fieldTypes[0] : new StructTypeImpl(canonicalIterNames, fieldTypes);
      return type;
    }

    int getTotalEntriesUpdated() {
      return initEntriesUpdated;
    }

    @Override
    public ObjectType getIndexResultSetType() {
      return indexResultSetType;
    }

    @Override
    public List getAllDependentIterators() {
      return fromIterators;
    }

    private ExecutionContext createExecutionContext(RegionEntry target)
        throws NameResolutionException, TypeMismatchException {
      DummyQRegion dQRegion = new DummyQRegion(rgn);
      dQRegion.setEntry(target);
      Object[] params = {dQRegion};
      ExecutionContext context = new ExecutionContext(params, cache);
      context.newScope(IndexCreationHelper.INDEX_QUERY_SCOPE_ID);

      if (dependencyGraph != null) {
        context.setDependencyGraph(dependencyGraph);
      }
      for (int i = 0; i < iteratorSize; i++) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) fromIterators.get(i);
        // We are re-using the same ExecutionContext on every evaluate -- this
        // is not how ExecutionContext was intended to be used.
        // Asif: Compute the dependency only once. The call to methods of this
        // class are thread safe as for update lock on Index is taken .
        if (dependencyGraph == null) {
          iterDef.computeDependencies(context);
        }
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.addToIndependentRuntimeItrMapForIndexCreation(iterDef);
        context.bindIterator(rIter);
      }
      // Save the dependency graph for future updates.
      if (dependencyGraph == null) {
        dependencyGraph = context.getDependencyGraph();
      }

      Support.Assert(indexResultSetType != null,
          "IMQEvaluator::evaluate:The StructType should have been initialized during index creation");
      return context;
    }

    public Object evaluateKey(Object object) {
      Object value = object;

      ExecutionContext newContext = null;
      Object key = null;
      try {
        if (object instanceof RegionEntry) {
          RegionEntry regionEntry = (RegionEntry) object;
          newContext = createExecutionContext(regionEntry);
          value = getTargetObjectForUpdate(regionEntry);
        }

        // context we use is the update context, from IMQEvaluator
        List iterators = newContext.getCurrentIterators();
        RuntimeIterator itr = (RuntimeIterator) iterators.get(0);
        itr.setCurrent(value);

        key = indexedExpr.evaluate(newContext);
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Could not reevaluate key for hash index");
        }
        throw new Error("Could not reevaluate key for hash index", e);
      }

      if (key == null) {
        key = IndexManager.NULL;
      }
      return key;
    }

    class HashIndexComparator implements Comparator {
      @Override
      public int compare(Object arg0, Object arg1) {
        // This comparator is used to sort results from the hash index
        // However some values may have been updated since being added to the result set.
        // In these cases UNDEFINED values could be present. So we
        // don't really care how UNDEFINED is sorted, in the end, this doesn't really matter
        // anyways.
        // We check to see if an update was in progress. If so (and is the way these turn to
        // undefined),
        // the value is reevaluated and removed from the result set if it does not match the
        // search criteria. This occurs in addToResultsFromEntries()
        Object key0 = ((Object[]) arg0)[0];
        Object key1 = ((Object[]) arg1)[0];

        Comparable comp0 = (Comparable) key0;
        Comparable comp1 = (Comparable) key1;
        return comp0.compareTo(comp1);
      }
    }

  }

  @Override
  void lockedQuery(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator indpndntItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException {
    lockedQueryPrivate(key, operator, results, iterOps, indpndntItr, context, null, projAttrib,
        intermediateResults, isIntersection);
  }

  @Override
  void lockedQuery(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    lockedQueryPrivate(key, operator, results, null, null, context, keysToRemove, null, null,
        true);
  }

  @Override
  void addMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    // TODO Auto-generated method stub

  }

  @Override
  void saveMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean isEmpty() {
    return entriesSet.isEmpty();
  }

  // public String printAll() {
  // return this.entriesSet.printAll();
  // }
}

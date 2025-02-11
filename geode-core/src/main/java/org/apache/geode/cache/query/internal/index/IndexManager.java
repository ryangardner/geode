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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexMaintenanceException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledPath;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.MapIndexable;
import org.apache.geode.cache.query.internal.NullToken;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.AbstractIndex.InternalIndexStatistics;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

public class IndexManager {
  private static final Logger logger = LogService.getLogger();

  public static final int ADD_ENTRY = 1;
  public static final int UPDATE_ENTRY = 2;
  public static final int REMOVE_ENTRY = 3;
  // Asif : This action is to rerun Index creation after
  // clear is called on the region
  public static final int RECREATE_INDEX = 4;
  private final InternalCache cache;
  protected final Region region;
  String indexThresholdSize =
      System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.INDEX_THRESHOLD_SIZE");

  private final boolean isOverFlowToDisk;
  private final boolean offHeap;
  private final boolean indexMaintenanceSynchronous;
  private int numCreators = 0;
  private int numUpdatersInProgress = 0;
  private int numUpdatersInWaiting = 0;
  private int iternameCounter = 0;
  /*
   * Map containing <IndexTask, FutureTask<IndexTask> or Index>. IndexTask represents an index thats
   * completely created or one thats in create phase. This is done in order to avoid synchronization
   * on the indexes.
   */
  private final ConcurrentMap indexes = new ConcurrentHashMap();
  // TODO Asif : Fix the appropriate size of the Map & the concurrency level
  private final ConcurrentMap canonicalizedIteratorNameMap = new ConcurrentHashMap();
  private IndexUpdaterThread updater;

  // Threshold for Queue.
  private final int INDEX_MAINTENANCE_BUFFER =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "AsynchIndexMaintenanceThreshold", -1);

  public static final boolean JOIN_OPTIMIZATION =
      !Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "index.DisableJoinOptimization");

  @MutableForTesting
  public static boolean INPLACE_OBJECT_MODIFICATION_FOR_TEST = false;

  @MutableForTesting
  public static boolean IS_TEST_LDM = false;

  @MutableForTesting
  public static boolean IS_TEST_EXPANSION = false;

  /**
   * System property to maintain the ReverseMap to take care in-place modification of the objects by
   * the application. In case of in-place modification the EntryEvent will not have the old-value,
   * without this the old-values are not removed from the index-maps thus resulting in inconsistent
   * results.
   */
  public static final boolean INPLACE_OBJECT_MODIFICATION = Boolean.parseBoolean(System.getProperty(
      GeodeGlossary.GEMFIRE_PREFIX + "index.INPLACE_OBJECT_MODIFICATION", "false"));

  /**
   * System property to turn-off the compact-index support.
   */
  public static final boolean RANGEINDEX_ONLY = Boolean.parseBoolean(
      System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "index.RANGEINDEX_ONLY", "false"));

  @MutableForTesting
  public static boolean TEST_RANGEINDEX_ONLY = false;
  public static final String INDEX_ELEMARRAY_THRESHOLD_PROP = "index_elemarray_threshold";
  public static final String INDEX_ELEMARRAY_SIZE_PROP = "index_elemarray_size";
  public static final int INDEX_ELEMARRAY_THRESHOLD =
      Integer.parseInt(System.getProperty(INDEX_ELEMARRAY_THRESHOLD_PROP, "100"));
  @MutableForTesting
  public static int INDEX_ELEMARRAY_THRESHOLD_FOR_TESTING = -1;

  public static final int INDEX_ELEMARRAY_SIZE =
      Integer.parseInt(System.getProperty(INDEX_ELEMARRAY_SIZE_PROP, "5"));
  @MakeNotStatic
  public static final AtomicLong SAFE_QUERY_TIME = new AtomicLong(0);
  @MutableForTesting
  public static boolean ENABLE_UPDATE_IN_PROGRESS_INDEX_CALCULATION = true;
  /** The NULL constant */
  public static final Object NULL = new NullToken();

  @MutableForTesting
  public static TestHook testHook;

  // private int numCreatorsInWaiting = 0;
  // @todo ericz
  // there should be a read/write lock PER INDEX in order to maximize
  // the concurrency of query execution.
  public IndexManager(InternalCache cache, Region region) {
    this.cache = cache;
    this.region = region;
    // must be a SortedMap to ensure the indexes are iterated over in fixed
    // order
    // to avoid deadlocks when acquiring locks
    // indexes = Collections.synchronizedSortedMap(new TreeMap());
    indexMaintenanceSynchronous = region.getAttributes().getIndexMaintenanceSynchronous();
    isOverFlowToDisk =
        region.getAttributes().getEvictionAttributes().getAction().isOverflowToDisk();
    offHeap = region.getAttributes().getOffHeap();
    if (!indexMaintenanceSynchronous) {
      updater = new IndexUpdaterThread(INDEX_MAINTENANCE_BUFFER,
          "OqlIndexUpdater:" + region.getFullPath());
      updater.start();
    }
  }

  /**
   * Stores the largest combination of current time + delta If there is a large delta/hiccup in
   * timings, this allows us to calculate the correct results for a query but, reevaluate more
   * aggressively. But the large hiccup will eventually be rolled off as time is always increasing
   * This is a fix for #47475
   *
   * @param lastModifiedTime the last modified time from version tag
   */
  public static void setIndexBufferTime(long lastModifiedTime, long currentCacheTime) {
    long oldValue = SAFE_QUERY_TIME.get();
    long newValue = currentCacheTime + currentCacheTime - lastModifiedTime;
    if (oldValue < newValue) {
      // use compare and set in case the value has been changed since we got the old value
      SAFE_QUERY_TIME.compareAndSet(oldValue, newValue);
    }
  }

  /**
   * only for test purposes This should not be called from any product code. Calls from product code
   * will possibly cause continous reevaluation (performance issue) OR incorrect query results
   * (functional issue)
   **/
  public static void resetIndexBufferTime() {
    SAFE_QUERY_TIME.set(0);
  }

  /**
   * Calculates whether we need to reevluate the key for the region entry We added a way to
   * determine whether to reevaluate an entry for query execution The method is to keep track of the
   * delta and current time in a single long value The value is then used by the query to determine
   * if a region entry needs to be reevaluated, based on subtracting the value with the query
   * execution time. This provides a delta + some false positive time (dts) If the dts + last
   * modified time of the region entry is > query start time, we can assume that it needs to be
   * reevaluated
   *
   * This is to fix bug 47475, where references to region entries can be held by the executing query
   * either directly or indirectly (iterators can hold references for next) but the values
   * underneath could change.
   *
   * Small amounts of false positives are ok as it will have a slight impact on performance
   */
  public static boolean needsRecalculation(long queryStartTime, long lastModifiedTime) {
    boolean needsRecalculate =
        (queryStartTime <= (lastModifiedTime + (SAFE_QUERY_TIME.get() - queryStartTime)));
    return ENABLE_UPDATE_IN_PROGRESS_INDEX_CALCULATION && needsRecalculate;
  }

  /** Test Hook */
  public interface TestHook {
    void hook(final int spot) throws RuntimeException;
  }

  /**
   * The Region this IndexManager is associated with
   *
   * @return the Region for this IndexManager
   */
  public Region getRegion() {
    return region;
  }

  /**
   * Used by tests to access the updater thread to determine its progress
   */
  public IndexUpdaterThread getUpdaterThread() {
    return updater;
  }

  // @todo need more specific list of exceptions
  /**
   * Create an index that can be used when executing queries.
   *
   * @param indexName the name of this index, used for statistics collection
   * @param indexType the type of index
   * @param origIndexedExpression the expression to index on, a function dependent on region entries
   *        individually.
   * @param origFromClause expression that evaluates to the collection(s) that will be queried over,
   *        must contain one and only one region path.
   * @return the newly created Index
   */
  public Index createIndex(String indexName, IndexType indexType, String origIndexedExpression,
      String origFromClause, String imports, ExecutionContext externalContext,
      PartitionedIndex prIndex, boolean loadEntries)
      throws IndexNameConflictException, IndexExistsException, IndexInvalidException {

    if (QueryMonitor.isLowMemory()) {
      throw new IndexInvalidException(
          "Index creation canceled due to low memory");
    }

    boolean oldReadSerialized = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);

    TXStateProxy tx = null;
    if (!cache.isClient()) {
      tx = ((TXManagerImpl) cache.getCacheTransactionManager()).pauseTransaction();
    }

    try {
      String projectionAttributes = "*"; // for now this is the only option

      if (getIndex(indexName) != null) {
        throw new IndexNameConflictException(
            String.format("Index named ' %s ' already exists.",
                indexName));
      }

      IndexCreationHelper helper = null;
      boolean isCompactOrHash = false;
      // Hash index not supported for overflow but we "thought" we were so let's maintain backwards
      // compatibility
      // and create a regular compact range index instead. This is due to having to reload entries
      // from overflow just
      // to recalculate the index key for the entry for comparisons during query.
      if (indexType == IndexType.HASH && isOverFlowRegion()) {
        indexType = IndexType.FUNCTIONAL;
      }
      if (indexType != IndexType.PRIMARY_KEY) {
        helper = new FunctionalIndexCreationHelper(origFromClause, origIndexedExpression,
            projectionAttributes, imports, (InternalCache) region.getCache(), externalContext,
            this);
        // Asif: For now support Map index as non compact .expand later
        // The limitation for compact range index also apply to hash index for now
        isCompactOrHash = shouldCreateCompactIndex((FunctionalIndexCreationHelper) helper);
      } else if (indexType == IndexType.PRIMARY_KEY) {
        helper = new PrimaryKeyIndexCreationHelper(origFromClause, origIndexedExpression,
            projectionAttributes, (InternalCache) region.getCache(), externalContext, this);
      } else {
        throw new AssertionError("Don't know how to set helper for " + indexType);
      }
      if (!isCompactOrHash && indexType != IndexType.PRIMARY_KEY) {

        if (indexType == IndexType.HASH) {
          if (!isIndexMaintenanceTypeSynchronous()) {
            throw new UnsupportedOperationException(
                "Hash index is currently not supported for regions with Asynchronous index maintenance.");
          }
          throw new UnsupportedOperationException(
              "Hash Index is not supported with from clause having multiple iterators(collections).");
        }
        // Overflow is not supported with range index.
        if (isOverFlowRegion()) {
          throw new UnsupportedOperationException(
              String.format(
                  "The specified index conditions are not supported for regions which overflow to disk. The region involved is %s",
                  region.getFullPath()));
        }
        // OffHeap is not supported with range index.
        if (isOffHeap()) {
          if (!isIndexMaintenanceTypeSynchronous()) {
            throw new UnsupportedOperationException(
                String.format(
                    "Asynchronous index maintenance is currently not supported for off-heap regions. The off-heap region is %s",
                    region.getFullPath()));
          }
          throw new UnsupportedOperationException(
              String.format(
                  "From clauses having multiple iterators(collections) are not supported for off-heap regions. The off-heap region is %s",
                  region.getFullPath()));
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Started creating index with indexName: {} On region: {}", indexName,
            region.getFullPath());
      }

      if (IndexManager.testHook != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("IndexManager TestHook is set.");
        }

        if (((LocalRegion) region).isInitialized()) {
          testHook.hook(1);
        } else {
          testHook.hook(0);
        }
      }

      IndexTask indexTask = new IndexTask(cache, indexName, indexType, origFromClause,
          origIndexedExpression, helper, isCompactOrHash, prIndex, loadEntries);
      FutureTask<Index> indexFutureTask = new FutureTask<>(indexTask);
      Object oldIndex = indexes.putIfAbsent(indexTask, indexFutureTask);

      Index index = null;

      boolean interrupted = false;
      try {
        if (oldIndex == null) {
          // Initialize index.
          indexFutureTask.run();
          // Set the index.
          index = indexFutureTask.get();
        } else {
          // Index with same name or characteristic already exists.
          // Check if index creation is complete.
          if (!(oldIndex instanceof Index)) {
            // Some other thread is creating the same index.
            // Wait for index to be initialized from other thread.
            ((Future) oldIndex).get();
          }

          // The Index is successfully created, throw appropriate error message
          // from this thread.
          if (getIndex(indexName) != null) {
            throw new IndexNameConflictException(
                String.format("Index named ' %s ' already exists.",
                    indexName));
          } else {
            throw new IndexExistsException(
                "Similar Index Exists");
          }
        }
      } catch (InterruptedException ignored) {
        interrupted = true;
      } catch (ExecutionException ee) {
        Throwable c = ee.getCause();
        if (c instanceof IndexNameConflictException) {
          throw (IndexNameConflictException) c;
        } else if (c instanceof IndexExistsException) {
          throw (IndexExistsException) c;
        } else if (c instanceof IMQException) {
          throw new IndexInvalidException(c.getMessage());
        }
        throw new IndexInvalidException(ee);

      } finally {
        // If the index is not successfully created, remove IndexTask from
        // the map.
        if (oldIndex == null && index == null) {
          Object ind = indexes.get(indexTask);
          if (ind != null && !(ind instanceof Index)) {
            indexes.remove(indexTask);
          }
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      assert (index != null);
      if (logger.isDebugEnabled()) {
        logger.debug("Completed creating index with indexName: {} On region: {}", indexName,
            region.getFullPath());
      }
      return index;

    } finally {
      cache.setPdxReadSerializedOverride(oldReadSerialized);
      ((TXManagerImpl) cache.getCacheTransactionManager()).unpauseTransaction(tx);

    }
  }

  /**
   * Return true if we should create CompactRangeIndex Required conditions: indexedExpression is a
   * path expression, fromClause has only one iterator and it is directly on the region values.
   * Currently we have to use the "fat" implementation when asynchronous index updates are on.
   */
  private boolean shouldCreateCompactIndex(FunctionalIndexCreationHelper helper) {
    if (RANGEINDEX_ONLY || TEST_RANGEINDEX_ONLY) {
      return false;
    }

    // compact range index is not supported on asynchronous index maintenance.
    // since compact range index maintains reference to region entry, in case of
    // asynchronous updates the window between cache operation updating the
    // index increases causing query thread to return new value before doing
    // index evaluation (resulting in wrong value. There is still a small window
    // which can be addressed by the sys property:
    // gemfire.index.acquireCompactIndexLocksWithRegionEntryLocks
    if (!getRegion().getAttributes().getIndexMaintenanceSynchronous()) {
      return false;
    }

    // indexedExpression requirement
    CompiledValue cv = helper.getCompiledIndexedExpression();
    int nodeType;
    do {
      nodeType = cv.getType();
      if (nodeType == CompiledValue.PATH) {
        cv = cv.getReceiver();
      }
    } while (nodeType == CompiledValue.PATH);
    // end of path, nodeType at this point should be an Identifier
    if (nodeType != OQLLexerTokenTypes.Identifier && nodeType != OQLLexerTokenTypes.METHOD_INV) {
      if (nodeType == OQLLexerTokenTypes.TOK_LBRACK && !helper.isMapTypeIndex()
          && helper.modifiedIndexExpr instanceof MapIndexable) {
        if (((MapIndexable) helper.modifiedIndexExpr).getIndexingKeys().size() == 1) {

        } else {
          return false;
        }
      } else {
        return false;
      }
    }

    // fromClause requirement
    List iterators = helper.getIterators();
    if (iterators.size() != 1) {
      return false;
    }
    // "missing link" must be "value". Later to support key, entry, etc.
    CompiledValue missingLink = helper.missingLink;
    if (helper.isFirstIteratorRegionEntry) {
      return true;
    } else if (!(missingLink instanceof CompiledPath)) {
      return false;

    }
    String tailId = ((CompiledPath) missingLink).getTailID();
    return tailId.equals("value") || tailId.equals("key");
  }

  public Index getIndex(String indexName) {
    IndexTask indexTask = new IndexTask(cache, indexName);
    Object ind = indexes.get(indexTask);
    // Check if the returned value is instance of Index (this means
    // the index is not in create phase, its created successfully).
    if (ind instanceof Index) {
      return (Index) ind;
    }
    return null;
  }

  public void addIndex(String indexName, Index index) {
    IndexTask indexTask = new IndexTask(cache, indexName);
    indexes.put(indexTask, index);
  }

  /**
   * Get the Index with the specified indexType, fromClause, indexedExpression TODO: Asif :Check if
   * synchronization is needed while obtaining Array of Indexes as similar to what we have used
   * during index updates. This function will get the exact index , if available, else will return
   * null
   *
   * @param indexType the type of index
   * @param definitions the String array containing the required defintions of the fromClause of the
   *        index
   * @param indexedExpression the indexedExpression for the index
   * @param context ExecutionContext
   * @return the sole index of the region with these parameters, or null if there isn't one
   */
  public IndexData getIndex(IndexType indexType, String[] definitions,
      CompiledValue indexedExpression, ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    IndexData indxData = null;
    int qItrSize = definitions.length;
    Iterator it = indexes.values().iterator();
    StringBuilder sb = new StringBuilder();
    indexedExpression.generateCanonicalizedExpression(sb, context);
    String indexExprStr = sb.toString();
    while (it.hasNext()) {
      int[] mapping = new int[qItrSize];
      Object ind = it.next();
      // Check if the returned value is instance of Index (this means
      // the index is not in create phase, its created successfully).
      if (!(ind instanceof Index)) {
        continue;
      }
      Index index = (Index) ind;
      if (!((IndexProtocol) ind).isMatchingWithIndexExpression(indexedExpression, indexExprStr,
          context) || index.getType() != indexType) {
        continue;
      }

      int matchLevel = getMatchLevel(definitions,
          ((IndexProtocol) index).getCanonicalizedIteratorDefinitions(), mapping);

      if (matchLevel == 0) {
        indxData = new IndexData((IndexProtocol) index, 0/* Exact Match */, mapping);
        break;
      }

    }
    return indxData;
  }

  public int compareIndexData(IndexType indexType, String[] indexDefinitions,
      String indexExpression, IndexType otherType, String[] otherDefinitions,
      String otherExpression, int[] mapping) {

    int matchLevel = -2;

    if (indexExpression.equals(otherExpression) && indexType == otherType) {
      /* Asif : A match level of zero means exact match. */
      matchLevel = getMatchLevel(otherDefinitions, indexDefinitions, mapping);
    }
    return matchLevel;
  }

  /**
   * Asif : Returns the best available Index based on the available iterators in the Group
   *
   * TODO: Asif :Check if synchronization is needed while obtaining Array of Indexes as similar to
   * what we have used during index updates
   *
   * @param indexType Primary or Range Index
   * @param definitions String array containing the canonicalized definitions of the Iterators of
   *        the Group
   * @param indexedExpression Index Expression path(CompiledValue) on which index needs to be
   *        created
   * @param context ExecutionContext object
   * @return IndexData object
   */
  public IndexData getBestMatchIndex(IndexType indexType, String[] definitions,
      CompiledValue indexedExpression, ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {

    Index bestIndex = null;
    Index bestPRIndex = null;
    int[] bestMapping = null;

    int qItrSize = definitions.length;
    int bestIndexMatchLevel = qItrSize;
    Iterator iter = indexes.values().iterator();
    StringBuilder sb = new StringBuilder();
    indexedExpression.generateCanonicalizedExpression(sb, context);
    String indexExprStr = sb.toString();
    PartitionedIndex prIndex = null;
    Index prevBestPRIndex = null;
    Index prevBestIndex = null;

    Index index;
    while (iter.hasNext()) {
      Object ind = iter.next();
      // Check if the value is instance of FutureTask, this means
      // the index is in create phase.
      if (ind instanceof FutureTask) {
        continue;
      }

      // If the index is still empty
      if (!((AbstractIndex) ind).isPopulated()) {
        continue;
      }

      index = (Index) ind;

      if (index instanceof PartitionedIndex) {
        prIndex = (PartitionedIndex) index;
        // Get one of the bucket index. This index will be
        // available on all the buckets.
        index = prIndex.getBucketIndex();
        if (index == null) {
          continue;
        }
      }

      // System.out.println(" Index = "+index);
      // Use simple strategy just pick first compatible index
      if (((IndexProtocol) index).isMatchingWithIndexExpression(indexedExpression, indexExprStr,
          context) && index.getType() == indexType) {

        // For PR the matched index needs to be available on all the query buckets.
        if (prIndex != null) {
          try {

            // Protect the PartitionedIndex from being removed when it is being used.
            if (!prIndex.acquireIndexReadLockForRemove()) {
              continue;
            }

            prIndex.verifyAndCreateMissingIndex(context.getBucketList());
          } catch (Exception ignored) {
            // Index is not there on all buckets.
            // ignore this index.
            prIndex.releaseIndexReadLockForRemove();
            prIndex = null;
            continue;
          }
        } else {
          // For index on replicated regions
          if (!((AbstractIndex) index).acquireIndexReadLockForRemove()) {
            continue;
          }
        }

        /*
         * Asif : A match level of zero means exact match. A match level greater than 0 means the
         * query from clauses have extra iterators as compared to Index resultset ( This does not
         * neccessarily mean that Index resultset is not having extra fields. It is just that the
         * criteria for match level is the absence or presence of extra iterators. The order of
         * preference will be 0 , <0 , > 0 for first cut.
         */
        String[] indexDefinitions = ((IndexProtocol) index).getCanonicalizedIteratorDefinitions();

        int[] mapping = new int[qItrSize];
        int matchLevel = getMatchLevel(definitions, indexDefinitions, mapping);

        if (matchLevel == 0) {
          prevBestPRIndex = bestPRIndex;
          bestPRIndex = prIndex;
          prevBestIndex = bestIndex;
          bestIndex = index;
          bestIndexMatchLevel = matchLevel;
          bestMapping = mapping;

          // If we chose new index we should release lock on previous index
          // chosen as bestIndex.
          if (prIndex != null && prevBestPRIndex != null
              && prevBestPRIndex instanceof PartitionedIndex) {
            ((PartitionedIndex) prevBestPRIndex).releaseIndexReadLockForRemove();
            prevBestPRIndex = null;
          } else if (prevBestIndex != null) {
            ((AbstractIndex) prevBestIndex).releaseIndexReadLockForRemove();
            prevBestIndex = null;
          }
          break;
        } else if ((bestIndexMatchLevel > 0 && matchLevel < bestIndexMatchLevel)
            || (bestIndexMatchLevel < 0 && matchLevel < 0 && matchLevel > bestIndexMatchLevel)) {
          prevBestPRIndex = bestPRIndex;
          bestPRIndex = prIndex;
          prevBestIndex = bestIndex;
          bestIndex = index;
          bestIndexMatchLevel = matchLevel;
          bestMapping = mapping;
        }

        // release the lock if this index is not chosen as bestIndex.
        if (prIndex != null && bestPRIndex != prIndex) {
          prIndex.releaseIndexReadLockForRemove();
          prIndex = null;
        } else if (bestIndex != index) {
          ((AbstractIndex) index).releaseIndexReadLockForRemove();
          index = null;
        }

        // If we chose new index we should release lock on previous index
        // chosen as bestIndex.
        if (prevBestPRIndex != null && prevBestPRIndex instanceof PartitionedIndex) {
          ((PartitionedIndex) prevBestPRIndex).releaseIndexReadLockForRemove();
          prevBestPRIndex = null;
        } else if (prevBestIndex != null) {
          ((AbstractIndex) prevBestIndex).releaseIndexReadLockForRemove();
          prevBestIndex = null;
        }
      }
    }
    if (bestIndex != null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "The best index found for index expression: {} is: {} with Match-level: {} and mapping: {}",
            indexExprStr, bestIndex, bestIndexMatchLevel, Arrays.toString(bestMapping));
      }
    }
    return bestIndex != null
        ? new IndexData((IndexProtocol) bestIndex, bestIndexMatchLevel, bestMapping) : null;
  }

  /*
   * Asif : This function returns the best match index. The crietria used to identify best match
   * index is based currently , relative to the query from clause. If the iterators of query from
   * clause exactly match the index from clause , then match level is zero & is the best match. If
   * the query from clause contain extra iterators , not available in index from clause, then mach
   * level is > 0 & is not the best. If the match level is < 0 that means Index from clause have
   * some extra iterators as compared to query. The int array gives the mapping of query iterator's
   * position relative to the index resultset fields . The position is '1' based index. That is for
   * the first query iterator ( 0 position), the mapping will contain 1 which can be thought of as
   * Index ResultSet value at the field with name index_iter1. If the second query iterator has a
   * value 3 , that means for (1 position) iterator , the field will have name index_iter3
   */
  private static int getMatchLevel(String[] queryDefintions, String[] indexDefinitions,
      int[] mapping) {
    int qItrLen = queryDefintions.length;
    int indxItrLen = indexDefinitions.length;
    // Asif : We know that because index expressions have matched that
    // itself indicates that the first iterator, which is regon iterator
    // has matched. So the index field position of the first RuntimeIterator
    // of the Query group is 1
    mapping[0] = 1;
    int matchLevel = qItrLen - 1;
    for (int i = 1; i < qItrLen; ++i) {
      for (int j = 1; j < indxItrLen; ++j) {
        if (queryDefintions[i].equals(indexDefinitions[j])) {
          mapping[i] = ++j;
          --matchLevel;
          break;
        }
      }
    }
    if (matchLevel == 0 && indxItrLen > qItrLen) {
      matchLevel = qItrLen - indxItrLen;
    }
    return matchLevel;
  }

  /*
   * private static int getMatchLevel(String fromClause, String iFromClause) { if
   * (fromClause.equals(iFromClause)) return 0; if (fromClause.startsWith(iFromClause)) { int cnt =
   * -1; int index = fromClause.indexOf(',', iFromClause.length() + 1); while (index > 0) { cnt--;
   * index = fromClause.indexOf(',', index + 1); } return cnt; } else if
   * (iFromClause.startsWith(fromClause)) { int cnt = 1; int index = iFromClause.indexOf(',',
   * fromClause.length() + 1); while (index > 0) { cnt++; index = iFromClause.indexOf(',', index +
   * 1); } return cnt; } //No compatible return Integer.MAX_VALUE; }
   */
  /**
   * Get a collection of all the indexes. If the IndexType is specified returns only the matching
   * indexes.
   *
   * @param indexType the type of indexes to get. Currently must be Indexable.FUNCTIONAL_SORTED
   * @return the collection of indexes for the specified region and type
   */
  public Collection getIndexes(IndexType indexType) {
    ArrayList<Index> list = new ArrayList<>();
    for (final Object ind : indexes.values()) {
      // Check if the value is instance of FutureTask, this means
      // the index is in create phase.
      if (ind instanceof FutureTask) {
        continue;
      }
      Index index = (Index) ind;

      // Check if indexType needs to be matched.
      if (indexType == null || index.getType() == indexType) {
        // No type check.
        list.add(index);
      }
    }
    return list;
  }

  /**
   * Get a collection of all the indexes managed by IndexManager
   *
   * @return the collection of indexes on the specified region
   */
  public Collection getIndexes() {
    return getIndexes(null);
  }

  /**
   * Remove the specified index.
   *
   * @param index the Index to remove
   */
  public void removeIndex(Index index) {
    if (index.getRegion() != region) {
      throw new IllegalArgumentException(
          "Index does not belong to this IndexManager");
    }
    // Asif: We will just remove the Index from the map. Since the
    // TreeMap is synchronized & the operation of adding a newly created
    // index is in synch there will not be any situation where the unintended
    // Index gets removed( in case of same Index Name scenario).
    // If query obtains the Index handle which is getting removed , that
    // is OK as we are not clearing data maps . The destroy though marks
    // the index invalid , that is OK. Because of this flag a query
    // may or may not use the Index
    IndexTask indexTask = new IndexTask(cache, index.getName());
    if (indexes.remove(indexTask) != null) {
      AbstractIndex indexHandle = (AbstractIndex) index;
      indexHandle.destroy();
    }
  }

  // @todo need more specific list of exceptions
  /**
   * Remove all the indexes managed by IndexManager
   */
  public int removeIndexes() {
    // Remove indexes which are available (create completed).
    int numIndexes = 0;
    for (final Object o : indexes.entrySet()) {
      Map.Entry entry = (Map.Entry) o;
      Object ind = entry.getValue();
      // Check if the returned value is instance of Index (this means
      // the index is not in create phase, its created successfully).
      if (!(ind instanceof Index)) {
        continue;
      }
      numIndexes++;
      IndexTask indexTask = (IndexTask) entry.getKey();
      indexes.remove(indexTask);
    }
    return numIndexes;
  }


  /**
   * Asif : This function is invoked during clear operation on Region. It causes re execution of
   * Index Initialization query on the region & before doing this it makes theexisting data maps
   * null. This is needed so that index does not miss any entry being put in the region when the
   * Region.clear is in progress
   */
  public void rerunIndexCreationQuery() throws QueryException {
    try {
      QueryObserver observer = QueryObserverHolder.getInstance();
      observer.beforeRerunningIndexCreationQuery();
    } catch (Exception e) {
      // Asif Ignore any exception as this should not hamper normal code flow
      if (logger.isDebugEnabled()) {
        logger.debug(
            "IndexMananger::rerunIndexCreationQuery: Exception in callback beforeRerunningIndexcreationQuery",
            e);
      }
    }
    if (isIndexMaintenanceTypeSynchronous()) {
      recreateAllIndexesForRegion();
    } else {
      // System.out.println("Aynchronous update");
      updater.addTask(RECREATE_INDEX, null, IndexProtocol.OTHER_OP);
    }
  }

  /**
   * populates all the indexes in the region
   */
  public void populateIndexes(Collection<Index> indexSet) throws MultiIndexCreationException {
    waitBeforeUpdate();
    if (region.getCache().getLogger().infoEnabled()) {
      region.getCache().getLogger().info("Populating indexes for region " + region.getName());
    }
    boolean throwException = false;
    HashMap<String, Exception> exceptionsMap = new HashMap<>();
    boolean oldReadSerialized = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);
    try {
      Iterator entryIter = ((LocalRegion) region).getBestIterator(true);
      while (entryIter.hasNext()) {
        RegionEntry entry = (RegionEntry) entryIter.next();
        if (entry == null || entry.isInvalidOrRemoved()) {
          continue;
        }
        // Fault in the value once before index update so that every index
        // update does not have
        // to read the value from disk every time.
        entry.getValue((LocalRegion) region);
        Iterator<Index> indexSetIterator = indexSet.iterator();
        while (indexSetIterator.hasNext()) {
          AbstractIndex index = (AbstractIndex) indexSetIterator.next();
          if (!index.isPopulated() && index.getType() != IndexType.PRIMARY_KEY) {
            if (logger.isDebugEnabled()) {
              logger.debug("Adding to index :{}{} value :{}", index.getName(),
                  region.getFullPath(), entry.getKey());
            }
            long start = index.updateIndexUpdateStats();
            try {
              index.addIndexMapping(entry);
            } catch (IMQException e) {
              if (logger.isDebugEnabled()) {
                logger.debug("Adding to index failed for: {}, {}", index.getName(), e.getMessage(),
                    e);
              }
              exceptionsMap.put(index.indexName, e);
              indexSetIterator.remove();
              throwException = true;
            }
            index.updateIndexUpdateStats(start);
          }
        }
      }
      setPopulateFlagForIndexes(indexSet);
      if (throwException) {
        throw new MultiIndexCreationException(exceptionsMap);
      }
    } finally {
      cache.setPdxReadSerializedOverride(oldReadSerialized);
      notifyAfterUpdate();
    }
  }

  /**
   * Sets the {@link AbstractIndex#isPopulated} after populating all the indexes in this region
   */
  public void setPopulateFlagForIndexes(Collection<Index> indexSet) {
    for (Object ind : indexSet) {
      AbstractIndex index = (AbstractIndex) ind;
      if (!index.isPopulated()) {
        index.setPopulated(true);
      }
    }
  }

  public void updateIndexes(RegionEntry entry, int action, int opCode) throws QueryException {
    updateIndexes(entry, action, opCode, false);
  }

  /**
   * Callback for IndexManager to update indexes Called from AbstractRegionMap.
   *
   * @param entry the RegionEntry being updated
   * @param action action to be taken (IndexManager.ADD_ENTRY, IndexManager.UPDATE_ENTRY,
   *        IndexManager.REMOVE_ENTRY)
   * @param opCode one of IndexProtocol.OTHER_OP, BEFORE_UPDATE_OP, AFTER_UPDATE_OP.
   */
  public void updateIndexes(RegionEntry entry, int action, int opCode,
      boolean isDiskRecoveryInProgress) throws QueryException {
    if (isDiskRecoveryInProgress) {
      assert !((LocalRegion) region).isInitialized();
    } else {
      assert Assert.assertHoldsLock(entry, true);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("IndexManager.updateIndexes {} + action: {}", entry.getKey(), action);
    }
    if (entry == null) {
      return;
    }
    if (isIndexMaintenanceTypeSynchronous()) {
      // System.out.println("Synchronous update");
      processAction(entry, action, opCode);
    } else {
      // System.out.println("Aynchronous update");
      updater.addTask(action, entry, opCode);
    }
  }

  /**
   * @param opCode one of IndexProtocol.OTHER_OP, BEFORE_UPDATE_OP, AFTER_UPDATE_OP.
   */
  private void processAction(RegionEntry entry, int action, int opCode) throws QueryException {
    final long startPA = getCachePerfStats().startIndexUpdate();
    Boolean initialPdxReadSerialized = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);
    TXStateProxy tx = null;
    if (!cache.isClient()) {
      tx = ((TXManagerImpl) cache.getCacheTransactionManager()).pauseTransaction();
    }

    try {
      // Asif: Allow the thread to update iff there is no current index
      // creator thread in progress. There will not be any issue if
      // allow the updater thread to proceed if there is any index
      // creator thread in waiting , but that can cause starvation
      // for index creator thread. So we will give priorityto index
      // creation thread
      if (IndexManager.testHook != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("IndexManager TestHook is set.");
        }
        testHook.hook(6); // ConcurrentIndexInitOnOverflowRegionDUnitTest
      }

      long start = 0;
      boolean indexLockAcquired = false;
      switch (action) {
        case ADD_ENTRY: {
          if (IndexManager.testHook != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("IndexManager TestHook in ADD_ENTRY.");
            }
            testHook.hook(5);
          }
          // this action is only called after update
          assert opCode == IndexProtocol.OTHER_OP;

          // Asif The behaviour can arise if an index creation has already
          // acted upon a newly added entry , but by the time callback
          // occurs , the index is added to the map & thus
          // the add operation will now have an effect of update.
          // so we need to remove the mapping even if it is an Add action
          // as otherwise the new results will get added into the
          // old results instead of replacement
          for (final Object ind : indexes.values()) {
            // Check if the value is instance of FutureTask, this means
            // the index is in create phase.
            if (ind instanceof FutureTask) {
              continue;
            }
            IndexProtocol index = (IndexProtocol) ind;

            if (index.isValid() && ((AbstractIndex) index).isPopulated()
                && index.getType() != IndexType.PRIMARY_KEY) {
              // Asif : If the current Index contains an entry inspite
              // of add operation , this can only mean that Index
              // has already acted on it during creation, so do not
              // apply IMQ on it
              if (!index.containsEntry(entry)) {
                if (logger.isDebugEnabled()) {
                  logger.debug("Adding to index: {}{} value: {}", index.getName(),
                      region.getFullPath(), entry.getKey());
                }
                start = ((AbstractIndex) index).updateIndexUpdateStats();
                addIndexMapping(entry, index);
                ((AbstractIndex) index).updateIndexUpdateStats(start);
              }
            }
          }
          break;
        }
        case UPDATE_ENTRY: {

          if (IndexManager.testHook != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("IndexManager TestHook in UPDATE_ENTRY.");
            }
            testHook.hook(5);
            testHook.hook(9); // QueryDataInconsistencyDUnitTest
          }

          // this action is only called with opCode AFTER_UPDATE_OP
          assert opCode == IndexProtocol.AFTER_UPDATE_OP;
          for (final Object ind : indexes.values()) {
            // Check if the value is instance of FutureTask, this means
            // the index is in create phase.
            if (ind instanceof FutureTask) {
              continue;
            }
            IndexProtocol index = (IndexProtocol) ind;

            if (((AbstractIndex) index).isPopulated() && index.getType() != IndexType.PRIMARY_KEY) {
              if (logger.isDebugEnabled()) {
                logger.debug("Updating index: {}{} value: {}", index.getName(),
                    region.getFullPath(), entry.getKey());
              }
              start = ((AbstractIndex) index).updateIndexUpdateStats();

              addIndexMapping(entry, index);

              ((AbstractIndex) index).updateIndexUpdateStats(start);
            }
          }
          break;
        }
        case REMOVE_ENTRY: {

          if (IndexManager.testHook != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("IndexManager TestHook in REMOVE_ENTRY.");
            }
            testHook.hook(5);
            testHook.hook(10);
          }
          for (final Object ind : indexes.values()) {
            // Check if the value is instance of FutureTask, this means
            // the index is in create phase.
            if (ind instanceof FutureTask) {
              continue;
            }
            IndexProtocol index = (IndexProtocol) ind;

            if (((AbstractIndex) index).isPopulated() && index.getType() != IndexType.PRIMARY_KEY) {
              AbstractIndex abstractIndex = (AbstractIndex) index;
              if (logger.isDebugEnabled()) {
                logger.debug("Removing from index: {}{} value: {}", index.getName(),
                    region.getFullPath(), entry.getKey());
              }
              start = ((AbstractIndex) index).updateIndexUpdateStats();

              removeIndexMapping(entry, index, opCode);

              ((AbstractIndex) index).updateIndexUpdateStats(start);
            }
          }
          break;
        }
        default: {
          throw new IndexMaintenanceException(
              "Invalid action");
        }
      }
    } finally {
      cache.setPdxReadSerializedOverride(initialPdxReadSerialized);
      ((TXManagerImpl) cache.getCacheTransactionManager()).unpauseTransaction(tx);

      getCachePerfStats().endIndexUpdate(startPA);
    }
  }

  void addIndexMapping(RegionEntry entry, IndexProtocol index) {
    try {
      index.addIndexMapping(entry);
    } catch (Exception exception) {
      index.markValid(false);
      setPRIndexAsInvalid((AbstractIndex) index);
      logger.warn(String.format(
          "Updating the Index %s failed. The index is corrupted and marked as invalid.",
          ((AbstractIndex) index).indexName), exception);
      logger.debug("Corrupted key is " + entry.getKey());
    }
  }

  void removeIndexMapping(RegionEntry entry, IndexProtocol index, int opCode) {
    try {
      index.removeIndexMapping(entry, opCode);
    } catch (Exception exception) {
      index.markValid(false);
      setPRIndexAsInvalid((AbstractIndex) index);
      logger.warn(String.format(
          "Updating the Index %s failed. The index is corrupted and marked as invalid.",
          ((AbstractIndex) index).indexName), exception);
      logger.debug("Corrupted key is " + entry.getKey());
    }
  }

  private void setPRIndexAsInvalid(AbstractIndex index) {
    if (index.prIndex != null) {
      AbstractIndex prIndex = (AbstractIndex) index.prIndex;
      prIndex.markValid(false);
    }
  }

  private void waitBeforeUpdate() {
    synchronized (indexes) {
      ++numCreators;
      // Asif : If there exists any updater thread in progress
      // we should not allow index creation to proceed.
      while (numUpdatersInProgress > 0) {
        ((LocalRegion) getRegion()).getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          indexes.wait();
        } catch (InterruptedException ignored) {
          interrupted = true;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // while
    }
  }

  private void notifyAfterUpdate() {
    synchronized (indexes) {
      --numCreators;
      // ASIF: If the creator is in progress , this itself
      // means that there is no Update active. The updater threads
      // are either in wait state or there are no threads at all.
      // Since we do not want any update to progress , if there is
      // any creator thread in lock seeking mode ( meaning that it has
      // entered the previous synch block) . We will not issue
      // any notify till the creator count drops to zero &
      // also unless there is any updater thread in waiting
      if (numCreators == 0 && numUpdatersInWaiting > 0) {
        indexes.notifyAll();
      }
    }
  }

  /**
   * Recreates all indexes for this region. This operation blocks all updates on all indexes while
   * recreate is in progress. This is required as recreate does NOT lock region entries before index
   * update and hence might cause inconsistencies in index if concurrent region entry operations are
   * going on.
   *
   */
  private void recreateAllIndexesForRegion() {

    long start = 0;
    waitBeforeUpdate();
    try {
      // opCode is ignored for this operation
      for (final Object ind : indexes.values()) {
        // Check if the value is instance of FutureTask, this means
        // the index is in create phase.
        if (ind instanceof FutureTask) {
          continue;
        }
        IndexProtocol index = (IndexProtocol) ind;
        if (index.getType() == IndexType.FUNCTIONAL || index.getType() == IndexType.HASH) {
          AbstractIndex aIndex = ((AbstractIndex) index);
          start = ((AbstractIndex) index).updateIndexUpdateStats();
          ((AbstractIndex) index).recreateIndexData();
          ((AbstractIndex) index).updateIndexUpdateStats(start);

        }
      }
    } catch (Exception e) {
      throw new IndexInvalidException(e);
    } finally {
      notifyAfterUpdate();
    }
  }

  /**
   * Wait for index initialization before entry create, update, invalidate or destroy operation.
   *
   * Note: If the region has a disk region then we should wait for index initialization before
   * getting region entry lock to avoid deadlock (#44431).
   */
  public void waitForIndexInit() {
    synchronized (indexes) {
      ++numUpdatersInWaiting;
      while (numCreators > 0) {
        ((LocalRegion) getRegion()).getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          indexes.wait();
        } catch (InterruptedException ignored) {
          interrupted = true;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // while
      --numUpdatersInWaiting;
      ++numUpdatersInProgress;
    }
  }

  /**
   * Necessary finally block call for above method.
   */
  public void countDownIndexUpdaters() {
    synchronized (indexes) {
      --numUpdatersInProgress;
      // Asif: Since Index creator threads can progress only if
      // there is no update threads in progress, thus we need to issue
      // notify all iff there are any creator threads in action & also
      // if the upDateInProgress Count has dipped to 0
      if (numUpdatersInProgress == 0 && numCreators > 0) {
        indexes.notifyAll();
      }
    }
  }

  private CachePerfStats getCachePerfStats() {
    return ((HasCachePerfStats) region).getCachePerfStats();
  }

  /**
   * Callback for destroying IndexManager Called after Region.destroy() called
   */
  public void destroy() throws QueryException {
    indexes.clear();
    if (!isIndexMaintenanceTypeSynchronous()) {
      updater.shutdown();
    }
  }

  /**
   * Removes indexes for a destroyed bucket region from the list of bucket indexes in the
   * {@link PartitionedIndex}.
   *
   * @param prRegion the partition region that this bucket belongs to
   */
  public void removeBucketIndexes(PartitionedRegion prRegion) throws QueryException {
    IndexManager parentManager = prRegion.getIndexManager();
    if (parentManager != null) {
      for (final Object o : indexes.values()) {
        Index bucketIndex = (Index) o;
        Index prIndex = parentManager.getIndex(bucketIndex.getName());
        if (prIndex instanceof PartitionedIndex) {
          ((PartitionedIndex) prIndex).removeFromBucketIndexes(region, bucketIndex);
        }
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (final Object ind : indexes.values()) {
      // Check if the value is instance of FutureTask, this means
      // the index is in create phase.
      if (ind instanceof FutureTask) {
        continue;
      }
      sb.append(ind).append(lineSeparator());
    }
    return sb.toString();
  }

  public boolean isIndexMaintenanceTypeSynchronous() {
    return indexMaintenanceSynchronous;
  }

  public boolean isOverFlowRegion() {
    return isOverFlowToDisk;
  }

  public boolean isOffHeap() {
    return offHeap;
  }

  public static boolean isObjectModificationInplace() {
    return (INPLACE_OBJECT_MODIFICATION || INPLACE_OBJECT_MODIFICATION_FOR_TEST);
  }

  /**
   * Asif : This function is used exclusively by Index Manager. It gets the unique Iterator name for
   * a Iterator definition, if it already exists, else creates a unqiue name & also stores it in a
   * map for subsequent use
   *
   * @param definition String containing definition of the iterator
   * @return String containing the name of the Iterator
   */
  public String putCanonicalizedIteratorNameIfAbsent(String definition) {
    String str = null;
    synchronized (canonicalizedIteratorNameMap) {
      if ((str = (String) canonicalizedIteratorNameMap.get(definition)) == null) {
        str = "index_iter" + getIncrementedCounter();
        String temp;
        if ((temp =
            (String) canonicalizedIteratorNameMap.putIfAbsent(definition, str)) != null) {
          str = temp;
        }
      }
    }
    return str;
  }

  public void putCanonicalizedIteratorName(String definition, String name) {
    synchronized (canonicalizedIteratorNameMap) {
      canonicalizedIteratorNameMap.put(definition, name);
    }
  }

  private synchronized int getIncrementedCounter() {
    return ++iternameCounter;
  }

  /**
   * Asif : Given a definition returns the canonicalized iterator name for the definition. If the
   * definition does not exist , null is returned
   *
   */
  public String getCanonicalizedIteratorName(String definition) {
    return ((String) (canonicalizedIteratorNameMap.get(definition)));
  }

  ////////////////////// Inner Classes //////////////////////

  public class IndexUpdaterThread extends LoggingThread {

    private volatile boolean running = true;

    private volatile boolean shutdownRequested = false;

    private final BlockingQueue pendingTasks;

    /**
     * Creates instance of IndexUpdaterThread
     */
    IndexUpdaterThread(int updateThreshold, String threadName) {
      super(threadName);
      // Check if threshold is set.
      if (updateThreshold > 0) {
        // Create a bounded queue.
        pendingTasks = new ArrayBlockingQueue(updateThreshold);
      } else {
        // Create non-bounded queue.
        pendingTasks = new LinkedBlockingQueue();
      }
    }

    public void addTask(int action, RegionEntry entry, int opCode) {
      Object[] task = new Object[3];
      task[0] = action;
      task[1] = entry;
      task[2] = opCode;
      pendingTasks.add(task);
    }

    /**
     * Stops this thread. Does not return until it has stopped.
     */
    public void shutdown() {
      if (!running) {
        return;
      }
      shutdownRequested = true;
      interrupt();
      try {
        join();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        // just return, we're done
      }
    }

    @Override
    public void run() {
      // async writers main loop
      // logger.debug("DiskRegion writer started (writer=" + this + ")");
      org.apache.geode.CancelCriterion stopper = ((LocalRegion) region).getCancelCriterion();
      try {
        while (!shutdownRequested) {
          // Termination checks
          SystemFailure.checkFailure();
          if (stopper.isCancelInProgress()) {
            break;
          }
          try {
            Object[] task = (Object[]) pendingTasks.take();
            if (shutdownRequested) {
              break;
            }
            updateIndexes(task);
          } catch (InterruptedException ignore) {
            return; // give up (exit the thread)
          }
        }
      } finally {
        running = false;
      }
    }

    private void updateIndexes(Object[] task) {
      int action = (Integer) task[0];
      RegionEntry entry = (RegionEntry) task[1];
      int opCode = (Integer) task[2];
      // System.out.println("entry = "+entry.getKey());
      if (entry != null || action == RECREATE_INDEX) {
        try {
          if (action == RECREATE_INDEX) {
            recreateAllIndexesForRegion();
          } else {
            if (entry != null) {
              entry.setUpdateInProgress(true);
            }
            processAction(entry, action, opCode);
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          if (entry != null && action != RECREATE_INDEX) {
            entry.setUpdateInProgress(false);
          }
        }
      }
    }

    /**
     * Used by tests to determine if the updater thread has finished updating its indexes. The list
     * is cleared without synchronization, which makes this methods somewhat unsafe from a threading
     * point of view.
     */
    public synchronized boolean isDone() {
      return pendingTasks.size() == 0;
    }

  }

  /**
   * Index Task used to create the index. This is used along with the FutureTask to take care of,
   * same index creation request from multiple threads. At any time only one thread succeeds and
   * other threads waits for the completion of the index creation. This avoids usage of
   * synchronization which could block any index creation.
   */
  public class IndexTask implements Callable<Index> {

    public String indexName;

    public IndexType indexType;

    public IndexCreationHelper helper;

    public String origFromClause;

    public String origIndexedExpression;

    public boolean isCompactOrHash = false;

    public boolean isLDM = false;

    public PartitionedIndex prIndex;

    public boolean loadEntries;

    private final InternalCache cache;

    IndexTask(InternalCache cache, String indexName, IndexType type, String origFromClause,
        String origIndexedExpression, IndexCreationHelper helper, boolean isCompactOrHash,
        PartitionedIndex prIndex, boolean loadEntries) {
      this.cache = cache;
      this.indexName = indexName;
      indexType = type;
      this.origFromClause = origFromClause;
      this.origIndexedExpression = origIndexedExpression;
      this.helper = helper;
      this.isCompactOrHash = isCompactOrHash;
      this.prIndex = prIndex;
      this.loadEntries = loadEntries;
    }

    /* For name based index search */
    IndexTask(InternalCache cache, String indexName) {
      this.cache = cache;
      this.indexName = indexName;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof IndexTask)) {
        return false;
      }
      IndexTask otherIndexTask = (IndexTask) other;
      // compare indexName.
      if (indexName.equals(otherIndexTask.indexName)) {
        return true;
      }

      if (otherIndexTask.helper == null || helper == null) {
        return false;
      }

      String[] indexDefinitions = helper.getCanonicalizedIteratorDefinitions();
      // TODO: avoid object creation in equals
      int[] mapping = new int[indexDefinitions.length];
      // compare index based on its type, expression and definition.
      return compareIndexData(indexType, indexDefinitions,
          helper.getCanonicalizedIndexedExpression(), otherIndexTask.indexType,
          otherIndexTask.helper.getCanonicalizedIteratorDefinitions(),
          otherIndexTask.helper.getCanonicalizedIndexedExpression(), mapping) == 0;
    }

    public int hashCode() {
      // It returns a constant number as the equality check is based on
      // the OR condition between the indexName and its characteristics
      // (involving type, expression and definition), because of this
      // its not possible to come-up with an accurate hash code.
      return 99;
    }

    /*
     * Creates and initializes the index.
     */
    @Override
    public Index call() {
      Index index = null;
      String indexedExpression = helper.getCanonicalizedIndexedExpression();
      String fromClause = helper.getCanonicalizedFromClause();
      String projectionAttributes = helper.getCanonicalizedProjectionAttributes();
      String[] definitions = helper.getCanonicalizedIteratorDefinitions();
      IndexStatistics stats = null;
      isLDM = IndexManager.IS_TEST_LDM;

      if (prIndex != null) {
        stats = prIndex.getStatistics();
      }
      if (indexType == IndexType.PRIMARY_KEY) {
        index = new PrimaryKeyIndex(cache, indexName, region, fromClause, indexedExpression,
            projectionAttributes, origFromClause, origIndexedExpression, definitions, stats);
        logger.info("Using Primary Key index implementation for '{}' on region {}", indexName,
            region.getFullPath());
      } else if (indexType == IndexType.HASH) {
        index = new HashIndex(cache, indexName, region, fromClause, indexedExpression,
            projectionAttributes, origFromClause, origIndexedExpression, definitions, stats);

        logger.info("Using Hash index implementation for '{}' on region {}", indexName,
            region.getFullPath());
      } else {
        // boolean isCompact = !helper.isMapTypeIndex() &&
        // shouldCreateCompactIndex((FunctionalIndexCreationHelper)helper);
        if (isCompactOrHash || isLDM) {
          if (indexType == IndexType.FUNCTIONAL && !helper.isMapTypeIndex()) {
            index = new CompactRangeIndex(cache, indexName, region, fromClause, indexedExpression,
                projectionAttributes, origFromClause, origIndexedExpression, definitions, stats);
            logger.info("Using Compact Range index implementation for '{}' on region {}", indexName,
                region.getFullPath());
          } else {
            FunctionalIndexCreationHelper fich = (FunctionalIndexCreationHelper) helper;
            index = new CompactMapRangeIndex(cache, indexName, region, fromClause,
                indexedExpression, projectionAttributes, origFromClause, origIndexedExpression,
                definitions, fich.isAllKeys(), fich.multiIndexKeysPattern, fich.mapKeys, stats);
            logger.info("Using Compact Map Range index implementation for '{}' on region {}",
                indexName, region.getFullPath());
          }
        } else {
          assert indexType == IndexType.FUNCTIONAL;
          if (!helper.isMapTypeIndex()) {
            index = new RangeIndex(cache, indexName, region, fromClause, indexedExpression,
                projectionAttributes, origFromClause, origIndexedExpression, definitions, stats);
            logger.info("Using Non-Compact index implementation for '{}' on region {}", indexName,
                region.getFullPath());
          } else {
            FunctionalIndexCreationHelper fich = (FunctionalIndexCreationHelper) helper;
            index = new MapRangeIndex(cache, indexName, region, fromClause, indexedExpression,
                projectionAttributes, origFromClause, origIndexedExpression, definitions,
                fich.isAllKeys(), fich.multiIndexKeysPattern, fich.mapKeys, stats);
            logger.info("Using Non-Compact Map index implementation for '{}' on region {}",
                indexName, region.getFullPath());
          }
        }
      }
      ((AbstractIndex) index).setPRIndex(prIndex);

      if (index.getType() != IndexType.PRIMARY_KEY) {
        AbstractIndex aIndex = ((AbstractIndex) index);
        aIndex.instantiateEvaluator(helper);
        waitBeforeUpdate();
        boolean indexCreatedSuccessfully = false;
        try {
          ((LocalRegion) region).setFlagForIndexCreationThread(true);
          aIndex.initializeIndex(loadEntries);
          logger.info((loadEntries ? "Initialized and loaded entries into the index "
              : "Initialized but entries not yet loaded into the index " + indexName
                  + " on region: " + region.getFullPath()));
          aIndex.markValid(true);
          indexCreatedSuccessfully = true;
          if (loadEntries) {
            aIndex.setPopulated(true);
            if (prIndex != null) {
              prIndex.setPopulated(true);
            }
          }
          indexes.put(this, index);
          if (region instanceof BucketRegion && prIndex != null) {
            prIndex.addToBucketIndexes(region, index);
            prIndex.incNumBucketIndexes();
          }
        } catch (Exception e) {
          throw new IndexInvalidException(e);
        } finally {
          notifyAfterUpdate();
          ((LocalRegion) region).setFlagForIndexCreationThread(false);
          if (!indexCreatedSuccessfully) {
            ((InternalIndexStatistics) index.getStatistics()).close();
          }
        }
      } else {
        // For PrimaryKey index
        ((AbstractIndex) index).setPopulated(true);
        indexes.put(this, index);
        if (region instanceof BucketRegion && prIndex != null) {
          prIndex.addToBucketIndexes(region, index);
        }
        if (prIndex != null) {
          prIndex.setPopulated(true);
        }
      }
      return index;
    }
  }
}

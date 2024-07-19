// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.utils.ExecutorThreadFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncDatabaseTransactionMgr extends DatabaseTransactionMgr{
    private static final Logger LOG = LogManager.getLogger(AsyncDatabaseTransactionMgr.class);

    private ScheduledExecutorService scheduledExecutorService;
    private LinkedBlockingDeque<TransactionOpRequest> txnRequestQueue = new LinkedBlockingDeque<>();
    public AsyncDatabaseTransactionMgr(long dbId, Env env, TransactionIdGenerator idGenerator) {
        super(dbId, env, idGenerator);
        scheduledExecutorService =
                new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("txn-opt-thread"));
        scheduledExecutorService.scheduleAtFixedRate(this::processBatchTxn, 0, 100, TimeUnit.MILLISECONDS);
    }

    private void processBatchTxn() {
        int size = txnRequestQueue.size();
        if (size == 0) {
            return;
        }
        List<TransactionOpRequest> requests = new ArrayList<>();
        List<TransactionState> states = new ArrayList<>();
        LOG.info("begin transaction request size: " + size);
        for (int i = 0; i < size; ++i) {
            TransactionOpRequest request = txnRequestQueue.pop();
            requests.add(request);
            states.add(request.getTransactionState());
        }
        boolean txnOperated = false;
        writeLock();
        try {
            LOG.info("process batch txn, batch size: " + size);
            unprotectedUpsertBatchTransactionState(states);
            txnOperated = true;
        } finally {
            writeUnlock();
            for (TransactionOpRequest request : requests) {
                TransactionOpResponse response = new TransactionOpResponse();
                if (txnOperated) {
                    response.setResultCode(TxnOperationResultCode.SUCCESS);
                } else {
                    response.setResultCode(TxnOperationResultCode.FAILED);
                    response.setErrMsg("begin txn failed!");
                }
                request.getFuture().complete(response);
            }
        }
    }

    private void unprotectedUpsertBatchTransactionState(List<TransactionState> transactionStates) {
        if (transactionStates.isEmpty()) {
            return;
        }
        // if this is a replay operation, we should not log it
        List<TransactionState> logStates = new ArrayList<>();
        for (TransactionState transactionState : transactionStates) {
            if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE
                    || transactionState.getSourceType() == TransactionState.LoadJobSourceType.FRONTEND) {
                logStates.add(transactionState);
            }
            if (!transactionState.getTransactionStatus().isFinalStatus()) {
                if (idToRunningTransactionState.put(transactionState.getTransactionId(), transactionState) == null) {
                    runningTxnNums++;
                }
            } else {
                if (idToRunningTransactionState.remove(transactionState.getTransactionId()) != null) {
                    runningTxnNums--;
                }
                idToFinalStatusTransactionState.put(transactionState.getTransactionId(), transactionState);
                if (transactionState.isShortTxn()) {
                    finalStatusTransactionStateDequeShort.add(transactionState);
                } else {
                    finalStatusTransactionStateDequeLong.add(transactionState);
                }
            }
            updateTxnLabels(transactionState);
        }
        if (!logStates.isEmpty()) {
            editLog.logBatchInsertTransactionState(logStates);
        }
    }

    @Override
    public long beginTransaction(List<Long> tableIdList, String label, TUniqueId requestId,
            TransactionState.TxnCoordinator coordinator, TransactionState.LoadJobSourceType sourceType,
            long listenerId, long timeoutSecond)
            throws DuplicatedRequestException, LabelAlreadyUsedException, BeginTransactionException,
            AnalysisException, QuotaExceedException, MetaNotFoundException {
        checkDatabaseDataQuota();
        readLock();
        try {
            checkForBeginTxn(requestId, label);
            checkRunningTxnExceedLimit();
        } finally {
            readLock();
        }

        long tid = idGenerator.getNextTransactionId();
        TransactionState transactionState = new TransactionState(dbId, tableIdList,
                tid, label, requestId, sourceType, coordinator, listenerId, timeoutSecond * 1000);
        transactionState.setPrepareTime(System.currentTimeMillis());
        try {
            CompletableFuture<TransactionOpResponse> future = new CompletableFuture<>();
            TransactionOpRequest request = new TransactionOpRequest(tid, TxnOperationType.BEGIN,
                    transactionState, future);
            txnRequestQueue.add(request);
            TransactionOpResponse response = future.get();
            if (response.getResultCode() == TxnOperationResultCode.FAILED) {
                throw new BeginTransactionException(response.getErrMsg());
            }
        } catch (Exception e) {
            throw new BeginTransactionException(e.getMessage());
        }
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
        }
        LOG.info("begin transaction: txn id {} with label {} from coordinator {}, listener id: {}",
                tid, label, coordinator, listenerId);
        return tid;
    }


    @Override
    public void preCommitTransaction2PC(List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        // check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }
        if (transactionState == null
                || transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionCommitFailedException(
                    transactionState == null ? "transaction not found" : transactionState.getReason());
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already visible: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction is already visible");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already committed: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction is already committed");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.PRECOMMITTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already pre-committed: {}", transactionId);
            }
            return;
        }

        Set<Long> errorReplicaIds = Sets.newHashSet();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        Map<Long, Set<Long>> tableToPartition = new HashMap<>();

        checkCommitStatus(tableList, transactionState, tabletCommitInfos, txnCommitAttachment, errorReplicaIds,
                tableToPartition, totalInvolvedBackends);

        asyncUnprotectedPreCommitTransaction2PC(transactionState, errorReplicaIds, tableToPartition,
                totalInvolvedBackends, db);
        try {
            CompletableFuture<TransactionOpResponse> future = new CompletableFuture<>();
            TransactionOpRequest request = new TransactionOpRequest(transactionId, TxnOperationType.PRE_COMMIT,
                    transactionState, future);
            txnRequestQueue.add(request);
            TransactionOpResponse response = future.get();
            if (response.getResultCode() == TxnOperationResultCode.FAILED) {
                throw new UserException(response.getErrMsg());
            }
        } catch (Exception e) {
            throw new UserException(e.getMessage());
        }

        LOG.info("transaction:[{}] successfully pre-committed", transactionState);
    }

    // like unprotectedPreCommitTransaction2PC, but don't persist transactionState in there
    private void asyncUnprotectedPreCommitTransaction2PC(TransactionState transactionState, Set<Long> errorReplicaIds,
            Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
            Database db) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        transactionState.setPreCommitTime(System.currentTimeMillis());
        transactionState.setTransactionStatus(TransactionStatus.PRECOMMITTED);
        transactionState.setErrorReplicas(errorReplicaIds);
        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            PartitionInfo tblPartitionInfo = table.getPartitionInfo();
            for (long partitionId : tableToPartition.get(tableId)) {
                String partitionRange = tblPartitionInfo.getPartitionRangeString(partitionId);
                PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(
                        partitionId, partitionRange, -1, -1,
                        table.isTemporaryPartition(partitionId));
                tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            }
            transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        // persist transactionState
        // unprotectUpsertTransactionState(transactionState, false);
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    @Override
    public void commitTransaction(List<Table> tableList, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
            TxnCommitAttachment txnCommitAttachment, Boolean is2PC)
            throws UserException {
        // check status
        // the caller method already own tables' write lock
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        checkTransactionStateBeforeCommit(db, tableList, transactionId, is2PC, transactionState);

        Set<Long> errorReplicaIds = Sets.newHashSet();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        Map<Long, Set<Long>> tableToPartition = new HashMap<>();
        if (!is2PC) {
            checkCommitStatus(tableList, transactionState, tabletCommitInfos, txnCommitAttachment, errorReplicaIds,
                    tableToPartition, totalInvolvedBackends);
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;
        try {
            if (is2PC) {
                asyncUnprotectedCommitTransaction2PC(transactionState, db);
            } else {
                asyncUnprotectedCommitTransaction(transactionState, errorReplicaIds,
                        tableToPartition, totalInvolvedBackends, db);
            }
            CompletableFuture<TransactionOpResponse> future = new CompletableFuture<>();
            TransactionOpRequest request = new TransactionOpRequest(transactionId, TxnOperationType.COMMIT,
                    transactionState, future);
            txnRequestQueue.add(request);
            TransactionOpResponse response = future.get();
            if (response.getResultCode() == TxnOperationResultCode.FAILED) {
                throw new BeginTransactionException(response.getErrMsg());
            }
            txnOperated = true;
        } catch (Exception e) {
            throw new UserException(e);
        } finally {
            // after state transform
            try {
                transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
            } catch (Throwable e) {
                LOG.warn("afterStateTransform txn {} failed. exception: ", transactionState, e);
            }
        }

        // update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db, false);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }


    // like unprotectedCommitTransaction2PC. but don't persist transactionState
    private void asyncUnprotectedCommitTransaction2PC(TransactionState transactionState, Database db) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PRECOMMITTED) {
            LOG.warn("Unknown exception. state of transaction [{}] changed, failed to commit transaction",
                    transactionState.getTransactionId());
            return;
        }
        // update transaction state version
        transactionState.setCommitTime(System.currentTimeMillis());
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);

        Iterator<TableCommitInfo> tableCommitInfoIterator
                = transactionState.getIdToTableCommitInfos().values().iterator();
        while (tableCommitInfoIterator.hasNext()) {
            TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            // table maybe dropped between commit and publish, ignore this error
            if (table == null) {
                tableCommitInfoIterator.remove();
                LOG.warn("table {} is dropped, skip and remove it from transaction state {}",
                        tableId,
                        transactionState);
                continue;
            }
            Iterator<PartitionCommitInfo> partitionCommitInfoIterator
                    = tableCommitInfo.getIdToPartitionCommitInfo().values().iterator();
            while (partitionCommitInfoIterator.hasNext()) {
                PartitionCommitInfo partitionCommitInfo = partitionCommitInfoIterator.next();
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                // partition maybe dropped between commit and publish version, ignore this error
                if (partition == null) {
                    partitionCommitInfoIterator.remove();
                    LOG.warn("partition {} is dropped, skip and remove it from transaction state {}",
                            partitionId,
                            transactionState);
                    continue;
                }
                partitionCommitInfo.setVersion(partition.getNextVersion());
                partitionCommitInfo.setVersionTime(System.currentTimeMillis());
            }
        }
        // persist transactionState
        // editLog.logInsertTransactionState(transactionState);
    }

    // like unprotectedCommitTransaction. but don't persist transactionState
    private void asyncUnprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
            Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
            Database db) {
        asyncCheckBeforeUnprotectedCommitTransaction(transactionState, errorReplicaIds);

        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            for (long partitionId : tableToPartition.get(tableId)) {
                Partition partition = table.getPartition(partitionId);
                tableCommitInfo.addPartitionCommitInfo(
                        generatePartitionCommitInfo(table, partitionId, partition.getNextVersion()));
            }
            transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        // persist transactionState
        // unprotectUpsertTransactionState(transactionState, false);
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    // like checkBeforeUnprotectedCommitTransaction, but don't persist transactionState
    private void asyncCheckBeforeUnprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        long commitTime = System.currentTimeMillis();
        transactionState.setCommitTime(commitTime);
        if (MetricRepo.isInit) {
            MetricRepo.HISTO_TXN_EXEC_LATENCY.update(commitTime - transactionState.getPrepareTime());
        }
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
        transactionState.setErrorReplicas(errorReplicaIds);

        // persist transactionState
        // unprotectUpsertTransactionState(transactionState, false);
    }


    @Override
    public void commitTransaction(long transactionId, List<Table> tableList,
            List<SubTransactionState> subTransactionStates) throws UserException {
        // check status
        // the caller method already own tables' write lock
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        if (DebugPointUtil.isEnable("DatabaseTransactionMgr.commitTransaction.failed")) {
            throw new TabletQuorumFailedException(transactionId,
                    "DebugPoint: DatabaseTransactionMgr.commitTransaction.failed");
        }

        checkTransactionStateBeforeCommit(db, tableList, transactionId, false, transactionState);

        // error replica may be duplicated for different sub transaction, but it's ok
        Set<Long> errorReplicaIds = Sets.newHashSet();
        Map<Long, Set<Long>> subTxnToPartition = new HashMap<>();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        for (SubTransactionState subTransactionState : subTransactionStates) {
            Map<Long, Set<Long>> tableToPartition = new HashMap<>();
            Table table = subTransactionState.getTable();
            List<TTabletCommitInfo> tabletCommitInfos = subTransactionState.getTabletCommitInfos();
            checkCommitStatus(Lists.newArrayList(table), transactionState,
                    TabletCommitInfo.fromThrift(tabletCommitInfos), null,
                    errorReplicaIds, tableToPartition, totalInvolvedBackends);
            Preconditions.checkState(tableToPartition.size() <= 1, "tableToPartition=" + tableToPartition);
            if (tableToPartition.size() > 0) {
                subTxnToPartition.put(subTransactionState.getSubTransactionId(), tableToPartition.get(table.getId()));
            }
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;
        try {
            ayncUnprotectedCommitTransaction(transactionState, errorReplicaIds, subTxnToPartition, totalInvolvedBackends,
                    subTransactionStates, db);

            CompletableFuture<TransactionOpResponse> future = new CompletableFuture<>();
            TransactionOpRequest request = new TransactionOpRequest(transactionId, TxnOperationType.COMMIT,
                    transactionState, future);
            txnRequestQueue.add(request);
            TransactionOpResponse response = future.get();
            if (response.getResultCode() == TxnOperationResultCode.FAILED) {
                throw new BeginTransactionException(response.getErrMsg());
            }
            txnOperated = true;
        } catch (Exception e) {
            throw new UserException(e);
        } finally {
            // after state transform
            try {
                transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
            } catch (Throwable e) {
                LOG.warn("afterStateTransform txn {} failed. exception: ", transactionState, e);
            }
        }

        // update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db, false);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }

    // like unprotectedCommitTransaction, but don't persist transactionState
    protected void ayncUnprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
            Map<Long, Set<Long>> subTxnToPartition, Set<Long> totalInvolvedBackends,
            List<SubTransactionState> subTransactionStates, Database db) {
        asyncCheckBeforeUnprotectedCommitTransaction(transactionState, errorReplicaIds);

        Map<Long, List<SubTransactionState>> tableToSubTransactionState = new HashMap<>();
        for (SubTransactionState subTransactionState : subTransactionStates) {
            long tableId = subTransactionState.getTable().getId();
            tableToSubTransactionState.computeIfAbsent(tableId, k -> new ArrayList<>()).add(subTransactionState);
        }

        for (Entry<Long, List<SubTransactionState>> entry : tableToSubTransactionState.entrySet()) {
            long tableId = entry.getKey();
            List<SubTransactionState> subTransactionStateList = entry.getValue();

            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            long tableNextVersion = table.getNextVersion();
            Map<Long, Long> partitionToVersion = new HashMap<>();

            for (SubTransactionState subTransactionState : subTransactionStateList) {
                Set<Long> partitionIds = subTxnToPartition.get(subTransactionState.getSubTransactionId());
                if (partitionIds == null) {
                    continue;
                }
                TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
                tableCommitInfo.setVersion(tableNextVersion);
                tableCommitInfo.setVersionTime(System.currentTimeMillis());

                for (long partitionId : partitionIds) {
                    long partitionNextVersion = table.getPartition(partitionId).getNextVersion();
                    if (partitionToVersion.containsKey(partitionId)) {
                        partitionNextVersion = partitionToVersion.get(partitionId) + 1;
                    }
                    partitionToVersion.put(partitionId, partitionNextVersion);

                    PartitionCommitInfo partitionCommitInfo = generatePartitionCommitInfo(table, partitionId,
                            partitionNextVersion);
                    tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
                    LOG.info("commit txn_id={}, sub_txn_id={}, partition_id={}, version={}",
                            transactionState.getTransactionId(), subTransactionState.getSubTransactionId(),
                            partitionId, partitionNextVersion);
                }
                transactionState.addSubTxnTableCommitInfo(subTransactionState, tableCommitInfo);
            }
        }
        // persist transactionState
        // unprotectUpsertTransactionState(transactionState, false);
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    // public void finishTransaction(long transactionId, Map<Long, Long> partitionVisibleVersions,
    //         Map<Long, Set<Long>> backendPartitions) throws UserException {
    //
    // }
    @Setter
    @Getter
    private static class TransactionOpRequest {
        private long transactionId;

        private TxnOperationType operationType;

        private TransactionState transactionState;

        private CompletableFuture<TransactionOpResponse> future;

        public TransactionOpRequest(long transactionId, TxnOperationType operationType,
                TransactionState transactionState, CompletableFuture<TransactionOpResponse> future) {
            this.transactionId = transactionId;
            this.operationType = operationType;
            this.transactionState = transactionState;
            this.future = future;
        }
    }

    @Setter
    @Getter
    private static class TransactionOpResponse {
        private long transactionId;
        private TxnOperationResultCode resultCode;
        private String errMsg;

    }

    private enum TxnOperationType {
        BEGIN,
        PRE_COMMIT,
        COMMIT,
        ABORT,
        FINISH,
    }

    private enum TxnOperationResultCode {
        SUCCESS,
        FAILED
    }
}

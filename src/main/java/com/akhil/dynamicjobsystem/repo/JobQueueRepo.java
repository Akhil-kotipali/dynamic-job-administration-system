package com.akhil.dynamicjobsystem.repo;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import com.akhil.dynamicjobsystem.enums.JobPriority;
import com.akhil.dynamicjobsystem.enums.JobStatus;
import com.akhil.dynamicjobsystem.models.JobQueue;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;

public interface JobQueueRepo extends JpaRepository<JobQueue, Long> {

    List<JobQueue> findByCurrentStatus(JobStatus currentStatus);

    List<JobQueue> findByPriority(JobPriority priority);

    List<JobQueue> findByPriorityAndCurrentStatusOrderByCreatedAtAsc(
            JobPriority priority,
            JobStatus currentStatus);

    boolean existsByCurrentStatus(JobStatus currentStatus);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({
            @QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")
    })
    Optional<JobQueue> findFirstByPriorityAndCurrentStatusOrderByCreatedAtAsc(
            JobPriority priority,
            JobStatus currentStatus);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT j FROM JobQueue j
            WHERE j.priority = :priority
            AND j.currentStatus = :status
            ORDER BY j.createdAt ASC
            """)
    List<JobQueue> findWaitingJobsForUpdate(
            @Param("priority") JobPriority priority,
            @Param("status") JobStatus status);

    List<JobQueue> findByCurrentStatusAndWorkStartedAtBefore(
            JobStatus status,
            LocalDateTime time);

    Optional<JobQueue> findByIdAndCurrentStatus(
            Long id,
            JobStatus status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT j FROM JobQueue j
            WHERE j.currentStatus = :status
            AND j.leaseExpiresAt <= :now
            ORDER BY j.leaseExpiresAt ASC
            """)
    List<JobQueue> findExpiredRunningJobsForUpdate(
            @Param("status") JobStatus status,
            @Param("now") LocalDateTime now);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT j FROM JobQueue j
            WHERE j.id = :id
            AND j.currentStatus = :status
            """)
    Optional<JobQueue> findByIdAndCurrentStatusForUpdate(
            @Param("id") Long id,
            @Param("status") JobStatus status);
}
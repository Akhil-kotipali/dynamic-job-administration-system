package com.akhil.dynamicjobsystem.service;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.akhil.dynamicjobsystem.enums.JobPriority;
import com.akhil.dynamicjobsystem.enums.JobStatus;
import com.akhil.dynamicjobsystem.models.JobQueue;
import com.akhil.dynamicjobsystem.repo.JobQueueRepo;

@Service
public class JobClaimService {

    private final JobQueueRepo queueRepo;

    private static final long LEASE_GRACE_MS = 10000;

    public JobClaimService(JobQueueRepo queueRepo) {
        this.queueRepo = queueRepo;
    }

    @Transactional
    public JobQueue claimNextJob(JobPriority priority) {

        JobQueue job = queueRepo
                .findFirstByPriorityAndCurrentStatusOrderByCreatedAtAsc(
                        priority, JobStatus.WAITING)
                .orElse(null);

        if (job == null) {
            return null;
        }

        LocalDateTime now = LocalDateTime.now();

        job.setCurrentStatus(JobStatus.RUNNING);
        job.setWorkStartedAt(now);
        job.setAttemptCount(job.getAttemptCount() + 1);

        // Lease lasts for the expected work duration plus a grace period
        job.setLeaseExpiresAt(
                now.plusNanos(
                        (job.getWorkDurationMs() + LEASE_GRACE_MS)
                                * 1_000_000L));

        return queueRepo.save(job);
    }

    @Transactional
    public boolean completeJob(Long jobId, int attemptToken) {

        JobQueue job = queueRepo
                .findByIdAndCurrentStatusForUpdate(
                        jobId, JobStatus.RUNNING)
                .orElse(null);

        if (job == null) {
            return false;
        }

        // Reject completion from an older attempt
        if (job.getAttemptCount() != attemptToken) {
            return false;
        }

        // Reject completion after this attempt's lease expired
        if (job.getLeaseExpiresAt() == null
                || !job.getLeaseExpiresAt().isAfter(LocalDateTime.now())) {
            return false;
        }

        // Change status to COMPLETED after work finishes
        job.setCurrentStatus(JobStatus.COMPLETED);
        job.setLeaseExpiresAt(null);

        queueRepo.save(job);

        return true;
    }

    @Transactional
    public int recoverExpiredJobs() {

        LocalDateTime now = LocalDateTime.now();

        List<JobQueue> jobs =
                queueRepo.findExpiredRunningJobsForUpdate(
                        JobStatus.RUNNING, now);

        int recovered = 0;

        for (JobQueue job : jobs) {

            if (job.getAttemptCount() < job.getMaxAttempts()) {

                job.setCurrentStatus(JobStatus.WAITING);
                job.setWorkStartedAt(null);
                job.setLeaseExpiresAt(null);

            } else {

                job.setCurrentStatus(JobStatus.FAILED);
                job.setLeaseExpiresAt(null);
            }

            queueRepo.save(job);
            recovered++;
        }

        return recovered;
    }
}
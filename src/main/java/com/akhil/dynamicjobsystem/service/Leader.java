package com.akhil.dynamicjobsystem.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.akhil.dynamicjobsystem.enums.JobStatus;
import com.akhil.dynamicjobsystem.repo.JobQueueRepo;

@Service
public class Leader {

    private final ThreadService threadService;
    private final JobQueueRepo queueRepo;
    private final JobClaimService jobClaimService;

    public Leader(
            ThreadService threadService,
            JobQueueRepo queueRepo,
            JobClaimService jobClaimService) {

        this.threadService = threadService;
        this.queueRepo = queueRepo;
        this.jobClaimService = jobClaimService;
    }

    // Method 1: Notify workers to check for waiting jobs
    @Scheduled(fixedDelay = 1000)
    public void assignJobs() {
        threadService.notifyWorkers();
    }

    // Method 2: Recover jobs that have exceeded the lease timeout
    @Scheduled(fixedDelay = 5000)
    public void manageFailures() {

        int recovered = jobClaimService.recoverExpiredJobs();

        if (recovered > 0) {
            threadService.notifyWorkers();
        }
    }

    // Method 3: Check whether waiting jobs exist
    @Scheduled(fixedDelay = 2000)
    public void monitorJobs() {

        if (queueRepo.existsByCurrentStatus(JobStatus.WAITING)) {
            threadService.notifyWorkers();
        }
    }
}
package com.akhil.dynamicjobsystem.controller;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.akhil.dynamicjobsystem.enums.JobPriority;
import com.akhil.dynamicjobsystem.enums.JobStatus;
import com.akhil.dynamicjobsystem.models.JobQueue;
import com.akhil.dynamicjobsystem.repo.JobQueueRepo;
import com.akhil.dynamicjobsystem.service.JobClaimService;
import com.akhil.dynamicjobsystem.service.ThreadService;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobQueueRepo queueRepo;
    private final ThreadService threadService;
    private final JobClaimService jobClaimService;

    public JobController(
            JobQueueRepo queueRepo,
            ThreadService threadService,
            JobClaimService jobClaimService) {

        this.queueRepo = queueRepo;
        this.threadService = threadService;
        this.jobClaimService = jobClaimService;
    }

    // Submit a new job
    @PostMapping
    public ResponseEntity<JobQueue> createJob(
            @RequestParam JobPriority priority,
            @RequestParam Long workDurationMs,
            @RequestParam(defaultValue = "3") int maxAttempts) {

        if (workDurationMs <= 0 || maxAttempts <= 0) {
            return ResponseEntity.badRequest().build();
        }

        JobQueue job = new JobQueue();

        job.setCreatedAt(LocalDateTime.now());
        job.setCurrentStatus(JobStatus.WAITING);
        job.setPriority(priority);
        job.setWorkDurationMs(workDurationMs);
        job.setAttemptCount(0);
        job.setMaxAttempts(maxAttempts);

        JobQueue savedJob = queueRepo.save(job);

        // Notify workers that a new job is available
        threadService.notifyWorkers();

        return ResponseEntity.ok(savedJob);
    }

    // Get a job by ID
    @GetMapping("/{id}")
    public ResponseEntity<JobQueue> getJob(@PathVariable Long id) {

        return queueRepo.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    // Get all jobs
    @GetMapping
    public List<JobQueue> getAllJobs() {
        return queueRepo.findAll();
    }

    // Get jobs by status
    @GetMapping("/status/{status}")
    public List<JobQueue> getJobsByStatus(
            @PathVariable JobStatus status) {

        return queueRepo.findByCurrentStatus(status);
    }

    // Get jobs by priority
    @GetMapping("/priority/{priority}")
    public List<JobQueue> getJobsByPriority(
            @PathVariable JobPriority priority) {

        return queueRepo.findByPriority(priority);
    }

    // Endpoint for testing failure recovery: create custom job in specific state
    @PostMapping("/test/create-custom")
    public ResponseEntity<JobQueue> createCustomJob(
            @RequestParam JobPriority priority,
            @RequestParam JobStatus status,
            @RequestParam int attemptCount,
            @RequestParam int maxAttempts,
            @RequestParam(required = false) Long leaseOffsetMs,
            @RequestParam(defaultValue = "3000") Long workDurationMs) {

        JobQueue job = new JobQueue();
        job.setCreatedAt(LocalDateTime.now());
        job.setCurrentStatus(status);
        job.setPriority(priority);
        job.setAttemptCount(attemptCount);
        job.setMaxAttempts(maxAttempts);
        job.setWorkDurationMs(workDurationMs);

        if (status == JobStatus.RUNNING) {
            job.setWorkStartedAt(LocalDateTime.now().minusSeconds(10));
            if (leaseOffsetMs != null) {
                job.setLeaseExpiresAt(LocalDateTime.now().plusNanos(leaseOffsetMs * 1_000_000L));
            } else {
                // Default: expired 5 seconds ago
                job.setLeaseExpiresAt(LocalDateTime.now().minusSeconds(5));
            }
        }

        JobQueue saved = queueRepo.save(job);
        return ResponseEntity.ok(saved);
    }

    // Endpoint for testing completion with attempt token (stale-worker check)
    @PostMapping("/test/complete")
    public ResponseEntity<Map<String, Object>> testCompleteJob(
            @RequestParam Long id,
            @RequestParam int attemptToken) {

        boolean success = jobClaimService.completeJob(id, attemptToken);
        return ResponseEntity.ok(Map.of("id", id, "attemptToken", attemptToken, "completed", success));
    }

    // Endpoint for testing explicit recovery trigger
    @PostMapping("/test/recover")
    public ResponseEntity<Map<String, Object>> testRecover() {
        int recovered = jobClaimService.recoverExpiredJobs();
        if (recovered > 0) {
            threadService.notifyWorkers();
        }
        return ResponseEntity.ok(Map.of("recoveredCount", recovered));
    }
}

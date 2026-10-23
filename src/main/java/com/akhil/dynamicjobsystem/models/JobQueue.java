package com.akhil.dynamicjobsystem.models;

import java.time.LocalDateTime;

import com.akhil.dynamicjobsystem.enums.JobPriority;
import com.akhil.dynamicjobsystem.enums.JobStatus;

import jakarta.persistence.*;

@Entity
@Table(name = "job_queue_table", indexes = {
    @Index(name = "idx_queue_claim", columnList = "priority, current_status, created_at")
})
public class JobQueue {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus currentStatus = JobStatus.WAITING;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobPriority priority = JobPriority.MEDIUM;

    @Column(nullable = false)
    private Long workDurationMs = 3000L;

    @Column(nullable = false)
    private int attemptCount = 0;

    @Column(nullable = false)
    private int maxAttempts = 3;

    private LocalDateTime workStartedAt;
    private LocalDateTime leaseExpiresAt;

    public JobQueue() {
    }

    @PrePersist
    void setDefaults() {
        if (createdAt == null)
            createdAt = LocalDateTime.now();
        if (currentStatus == null)
            currentStatus = JobStatus.WAITING;
        if (priority == null)
            priority = JobPriority.MEDIUM;
        if (workDurationMs == null)
            workDurationMs = 3000L;
        if (maxAttempts <= 0)
            maxAttempts = 3;
    }

    public Long getId() {
        return id;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public JobStatus getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(JobStatus currentStatus) {
        this.currentStatus = currentStatus;
    }

    public JobPriority getPriority() {
        return priority;
    }

    public void setPriority(JobPriority priority) {
        this.priority = priority;
    }

    public Long getWorkDurationMs() {
        return workDurationMs;
    }

    public void setWorkDurationMs(Long workDurationMs) {
        this.workDurationMs = workDurationMs;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public LocalDateTime getWorkStartedAt() {
        return workStartedAt;
    }

    public void setWorkStartedAt(LocalDateTime workStartedAt) {
        this.workStartedAt = workStartedAt;
    }

    public LocalDateTime getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public void setLeaseExpiresAt(LocalDateTime leaseExpiresAt) {
        this.leaseExpiresAt = leaseExpiresAt;
    }
}
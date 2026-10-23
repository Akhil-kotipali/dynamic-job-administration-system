package com.akhil.dynamicjobsystem.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.akhil.dynamicjobsystem.enums.JobPriority;
import com.akhil.dynamicjobsystem.models.JobQueue;

import jakarta.annotation.PreDestroy;

@Service
public class ThreadService {

    private static final Logger log = LoggerFactory.getLogger(ThreadService.class);

    private final JobClaimService jobs;
    private final ExecutorService high = Executors.newFixedThreadPool(3);
    private final ExecutorService medium = Executors.newFixedThreadPool(2);
    private final ExecutorService low = Executors.newFixedThreadPool(1);
    private final Object monitor = new Object();
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ThreadService(JobClaimService jobs) {
        this.jobs = jobs;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startWorkers() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        high.submit(() -> work("H1", JobPriority.HIGH, null, null)); // H1: reserved
        high.submit(() -> work("H2", JobPriority.HIGH, JobPriority.MEDIUM, JobPriority.LOW));
        high.submit(() -> work("H3", JobPriority.HIGH, JobPriority.MEDIUM, JobPriority.LOW));
        medium.submit(() -> work("M1", JobPriority.MEDIUM, null, null)); // M1: reserved
        medium.submit(() -> work("M2", JobPriority.MEDIUM, JobPriority.HIGH, JobPriority.LOW));
        low.submit(() -> work("L1", JobPriority.LOW, JobPriority.HIGH, JobPriority.MEDIUM));
    }

    public void notifyWorkers() {
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private void work(String workerName, JobPriority first, JobPriority second, JobPriority third) {
        Thread.currentThread().setName(workerName);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                JobQueue job = jobs.claimNextJob(first);
                if (job == null && second != null) {
                    job = jobs.claimNextJob(second);
                }
                if (job == null && third != null) {
                    job = jobs.claimNextJob(third);
                }

                if (job == null) {
                    synchronized (monitor) {
                        monitor.wait(500);
                    }
                    continue;
                }

                Thread.sleep(job.getWorkDurationMs());
                jobs.completeJob(job.getId(), job.getAttemptCount());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return; // Lease recovery will handle the RUNNING job later.
            } catch (Exception e) {
                log.error("Worker [{}] encountered an unexpected exception during job processing",
                        Thread.currentThread().getName(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    @PreDestroy
    public void stopWorkers() {
        high.shutdownNow();
        medium.shutdownNow();
        low.shutdownNow();
    }
}
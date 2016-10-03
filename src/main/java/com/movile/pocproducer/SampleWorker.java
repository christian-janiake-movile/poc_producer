package com.movile.pocproducer;

import com.movile.pgle.PeerGroup;
import com.movile.pgle.listener.Worker;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SampleWorker implements Worker, Runnable {

    private PeerGroup peerGroup;
    private boolean onDuty = false;

    private Date lastDatabaseQuery;
    private AtomicInteger lastRowRead = new AtomicInteger(0);

    private Integer carrierId;
    private String paginationTable;

    public SampleWorker(PeerGroup peerGroup) {
        this.peerGroup = peerGroup;
        this.carrierId = Integer.parseInt(peerGroup.getProperties().getProperty("engine.carrierId"));
        this.paginationTable = peerGroup.getProperties().getProperty("engine.paginationTable");
    }

    public void work() throws InterruptedException {
        System.out.println("WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK WORK");
        this.onDuty = true;
        synchronized(this) {
            this.notify();
        }
    }

    public void stop() {
        System.out.println("STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP STOP");
        this.onDuty = false;
    }

    public void run() {

        while(true) {

            synchronized(this) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            while(onDuty) {

                System.out.println(peerGroup.getName() + " is working. ON DUTY: " + onDuty);
                try {
                    Thread.sleep(500l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}

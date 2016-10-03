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
        onDuty = true;
    }

    public void stop() {
        onDuty = false;
    }

    public void run() {
        while(true) {
            if(onDuty) {
                System.out.println(peerGroup.getName() + " is working");
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

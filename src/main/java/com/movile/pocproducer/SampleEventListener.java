package com.movile.pocproducer;

import com.movile.pgle.PeerGroup;
import com.movile.pgle.listener.EventListener;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class SampleEventListener extends EventListener {

    @Autowired
    Logger log;

    private Date lastDatabaseQuery;
    private AtomicInteger lastRowRead = new AtomicInteger(0);

    private Integer carrierId;
    private String paginationTable;

    @Override
    public void peerGroupRegister(PeerGroup peerGroup) {

    }

    @Override
    public void isLeader(PeerGroup peerGroup) {

    }

    @Override
    public void isNotLeader(PeerGroup peerGroup) {

    }
}

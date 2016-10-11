package com.movile.pocproducer;

import com.movile.pgle.Coordinator;
import com.movile.pgle.PeerGroup;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Properties;

/**
 * Created by vagrant on 03/10/16.
 */
public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(Coordinator.class);
        ctx.register(SampleWorker.class);
        ctx.register(SecurityCache.class);
        ctx.register(SystemResources.class);
        ctx.refresh();

//        Registerer registerer = (Registerer) ctx.getBean(Registerer.class);
//        registerer.initialize();

        Properties properties = new Properties();
        properties.setProperty("group.name", "teste");
        properties.setProperty("group.electionInterval", "3000");
        properties.setProperty("group.leadershipInterval", "60000");
        properties.setProperty("group.workerClass", "sampleWorker"); //"com.movile.pocproducer.SampleWorker");
        properties.setProperty("engine.carrierId", "1");
        properties.setProperty("engine.paginationTable", "temporary.hybrid_engine_1");
        properties.setProperty("engine.refreshIntervalMinutes", "15");
        properties.setProperty("engine.usersPerCycle", "100");
        properties.setProperty("engine.usersBatchSize", "10");

        PeerGroup peerGroup = (PeerGroup) ctx.getBean("peerGroup", 1, properties);
    }
}

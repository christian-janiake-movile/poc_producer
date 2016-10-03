package com.movile.pocproducer;

import com.movile.pgle.Coordinator;
import com.movile.pgle.PeerGroup;
import com.movile.pgle.Registerer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by vagrant on 03/10/16.
 */
public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(Coordinator.class);
        ctx.refresh();

//        Registerer registerer = (Registerer) ctx.getBean(Registerer.class);
//        registerer.initialize();

        PeerGroup peerGroup = (PeerGroup) ctx.getBean("peerGroup", 1, "teste", 5000l, 30000l, "com.movile.pocproducer.SampleWorker");
    }
}

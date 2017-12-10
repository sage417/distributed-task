package moe.yamato.task.worker;

import moe.yamato.task.worker.master.MasterWatcher;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Names.bindProperties(binder, properties)
        MasterWatcher mw = new MasterWatcher("sagedeMac-mini:2181");
        mw.startZk();
        mw.runForMaster();
        TimeUnit.SECONDS.sleep(6);
        mw.stopZk();
    }
}

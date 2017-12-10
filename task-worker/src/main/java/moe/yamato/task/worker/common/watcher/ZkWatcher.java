package moe.yamato.task.worker.common.watcher;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public abstract class ZkWatcher implements Watcher {

    protected ZooKeeper zk;

    protected String hostPort;

    protected ZkWatcher(final String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(this.hostPort, (int) TimeUnit.SECONDS.toMillis(15), this);
    }

    public void stopZk() throws InterruptedException {
        this.zk.close();
    }
}

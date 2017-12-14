package moe.yamato.task.worker.worker;

import moe.yamato.task.worker.common.watcher.ZkWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.UUID;

class WorkerWatcher extends ZkWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerWatcher.class);

    private static final String serverId = UUID.randomUUID().toString();

    @Inject
    WorkerWatcher(String hostPort) {
        super(hostPort);
    }

    public static void main(String args[]) throws Exception {
        WorkerWatcher w = new WorkerWatcher("sagedeMac-mini:2181");
        w.startZk();
        w.register();
        Thread.sleep(30000);
    }

    @Override
    public void process(WatchedEvent e) {
        LOGGER.info(e.toString() + ", " + hostPort);
    }

    void register() {
        zk.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                (rc, path, ctx, name) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            register();
                            break;
                        case OK:
                            LOGGER.info("Registered successfully: " + serverId);
                            break;
                        case NODEEXISTS:
                            LOGGER.warn("Already registered: " + serverId);
                            break;
                        default:
                            LOGGER.error("Something went wrong: "
                                    + KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                }, null);
    }
}

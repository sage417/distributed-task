package moe.yamato.task.worker.master;

import moe.yamato.task.worker.common.watcher.ZkWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class MasterWatcher extends ZkWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterWatcher.class);

    private String serverId = "1";

    private boolean isMaster;

    @Inject
    public MasterWatcher(final String hostPort) {
        super(hostPort);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public void runForMaster() {
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, (rc, path, ctx, name) -> {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    isMaster = true;
                    break;
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                default:
                    isMaster = false;
                    break;
            }
        }, null);
    }

    private void checkMaster() {
        zk.getData("/master", false, (rc, path, ctx, data, stat) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
            }
        }, null);
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                (rc, p, ctx, name) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            createParent(p, (byte[]) ctx);
                            break;
                        case OK:
                            break;
                        case NODEEXISTS:
                            break;
                        default:
                    }
                },
                data);
    }
}

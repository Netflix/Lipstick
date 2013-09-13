package com.netflix.lipstick.hivetolipstick;

import org.apache.hadoop.hive.ql.hooks.HookContext;

public interface H2LClient {
    void preExecute(HookContext hookContext);
    void postExecute();
    String getPlanId();
}

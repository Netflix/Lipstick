package com.netflix.lipstick.hivetolipstick;

import org.apache.hadoop.hive.ql.hooks.HookContext;

/**
 * An interface for H2LClient
 * @author nbates
 *
 */
public interface H2LClient {
    void preExecute(HookContext hookContext);
    void postExecute();
}

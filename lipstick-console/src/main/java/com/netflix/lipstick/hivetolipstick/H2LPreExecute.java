package com.netflix.lipstick.hivetolipstick;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
/**
 * A class to be invoked by the hive framework.
 * To use, specify:  --hiveconf hive.exec.pre.hooks=com.netflix.lipstick.hivetolipstick.H2LPreExecute
 * @author nbates
 *
 */
public class H2LPreExecute implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {
        BasicH2LClient.getInstance().preExecute(hookContext);
    }
}

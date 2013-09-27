package com.netflix.lipstick.hivetolipstick;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

/**
 * A class to be invoked by the hive framework.
 * To use, specify:  --hiveconf hive.exec.post.hooks=com.netflix.lipstick.hivetolipstick.H2LPostExecute
 * @author nbates
 *
 */
public class H2LPostExecute implements ExecuteWithHookContext {
    @Override
    public void run(HookContext context) throws Exception {
        BasicH2LClient.getInstance().postExecute();
    }
}

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
    protected static final String LIPSTICK_URL_PROP = "lipstick.server.url";

    @Override
    public void run(HookContext context) throws Exception {
        String serviceUrl = context.getConf().getAllProperties().getProperty(LIPSTICK_URL_PROP);
        BasicH2LClient client = BasicH2LClient.getInstance(serviceUrl);
        client.postExecute();
    }
}

package com.netflix.lipstick.listeners;

/**
 * Lipstick Pig Progress notification listener Error Handler
 *
 * For finer grain error hanlding from the LipstickPPNL, implement
 * this interface, then pass an instance to LipstickPPNL.addErrorHandler()
 */
public interface PPNLErrorHandler {

    /**
     * Called when an unhandled excepction from LipstickPPNL.setPlanGenerators() occurs
     */
    public void handlePlanGeneratorsError(Exception e);

    /**
     * Called when an unhandled excepction from LipstickPPNL.initialPlanNotification() occurs
     */
    public void handleInitialPlanNotificationError(Exception e);

    /**
     * Called when an unhandled excepction from LipstickPPNL.jobStartedNotification() occurs
     */
    public void handleJobStartedNotificationError(Exception e);

    /**
     * Called when an unhandled excepction from LipstickPPNL.jobFinishedNotification() occurs
     */
    public void handleJobFinishedNotificationError(Exception e);

    /**
     * Called when an unhandled excepction from LipstickPPNL.jobFailedNotification() occurs
     */
    public void handleJobFailedNotificationError(Exception e);

    /**
     * Called when an unhandled excepction from LipstickPPNL.progressUpdatedNotification() occurs
     */
    public void handleProgressUpdatedNotificationError(Exception e);

    /**
     * Called when an unhandled excepction from LipstickPPNL.launchCompletedNotification() occurs
     */
    public void handleLaunchCompletedNotificationError(Exception e);

}


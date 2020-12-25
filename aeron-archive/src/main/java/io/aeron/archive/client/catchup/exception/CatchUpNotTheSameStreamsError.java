package io.aeron.archive.client.catchup.exception;

/**
 * This error happens when we realised that those streams which we tried to merge while a catchUp
 * process contains different messages
 */
public class CatchUpNotTheSameStreamsError extends Error {

    public CatchUpNotTheSameStreamsError() {
        super("Looks like we try to merge different streams!");
    }
}

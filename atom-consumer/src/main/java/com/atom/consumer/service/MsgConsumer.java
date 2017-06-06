package com.atom.consumer.service;

/**
 * received Msg
 * <p>
 * Created by Atom on 2017/5/27.
 */
public interface MsgConsumer {

    void receivedMsg(String msg);

}

package me.binge.redis.utils;

import java.util.Arrays;

public class RedisCmdPair {

    private String cmd;
    private Object[] oArgs; // key and values
    private RedisCmdPair cmdPair;

    public RedisCmdPair getCmdPair() {
        return cmdPair;
    }

    public void setCmdPair(RedisCmdPair cmdPair) {
        this.cmdPair = cmdPair;
    }

    public RedisCmdPair() {
    }

    public RedisCmdPair(String cmd, Object[] oArgs) {
        this.cmd = cmd;
        this.oArgs = oArgs;
    }

    public String getCmd() {
        return cmd;
    }

    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    public Object[] getoArgs() {
        return oArgs;
    }

    public void setoArgs(Object[] oArgs) {
        this.oArgs = oArgs;
    }

    @Override
    public String toString() {
        return "RedisCmdPair [cmd=" + cmd + ", oArgs=" + Arrays.toString(oArgs)
                + "]";
    }

}

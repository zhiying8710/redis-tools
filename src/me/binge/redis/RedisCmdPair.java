package me.binge.redis;

public class RedisCmdPair {

    private String cmd;
    private Object[] oArgs; // key and values

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

}

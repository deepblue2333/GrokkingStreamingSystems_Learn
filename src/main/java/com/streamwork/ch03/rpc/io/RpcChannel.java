/*
 * UserClientHandler.java

 */

package com.streamwork.ch03.rpc.io;

import com.streamwork.ch03.rpc.common.RpcRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Data;

import java.util.concurrent.Callable;

/**
 * @author razertory
 * @date 2021/1/6
 */
@Data
public class RpcChannel extends ChannelInboundHandlerAdapter implements Callable {

    private ChannelHandlerContext context;
    private String result;
    private RpcRequest request;
    private int serverPort;
    private String serverAddress;

    public RpcChannel(String ipaddress, int serverPort) {
        this.serverPort = serverPort;
        this.serverAddress = ipaddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.context = ctx;
    }

    @Override
    public synchronized void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        result = msg.toString();
        notify();
    }

    public synchronized Object call() throws Exception {
        context.writeAndFlush(request);
        wait();
        return result;
    }

    public void setRequest(RpcRequest request) {
        this.request = request;
    }
}

/*
 * RpcDecoder.java

 */

package com.streamwork.ch03.rpc.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RpcDecoder extends ByteToMessageDecoder {

    private Class<?> clazz;
    private RpcSerializer rpcSerializer;

    public RpcDecoder(Class<?> clazz, RpcSerializer rpcSerializer) {
        this.clazz = clazz;
        this.rpcSerializer = rpcSerializer;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
        List<Object> list) throws Exception {
        System.out.println("readableBytes before read: " + byteBuf.readableBytes());
        // 读取传送过来的消息的长度
        int dataLength = byteBuf.readInt();
        System.out.println("dataLength: " + dataLength);
        System.out.println("readableBytes after read: " + byteBuf.readableBytes());


        // 我们读到的消息体长度为 0，这是不应该出现的情况，这里出现这情况，关闭连接。
        if (dataLength < 0) {
            channelHandlerContext.close();
        }

        //取出数据
        System.out.println("byte");
        byte[] bytes = new byte[dataLength];

        byteBuf.readBytes(bytes);  //
        System.out.println("byte");

        System.out.println("dataLength: " + dataLength);
        System.out.println("readableBytes: " + byteBuf.readableBytes());

        //将 byte 数据转化为我们需要的对象。
        Object o = rpcSerializer.deserialize(clazz, bytes);
        list.add(o);
    }
}

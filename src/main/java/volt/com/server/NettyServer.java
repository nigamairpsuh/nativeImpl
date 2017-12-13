package volt.com.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import volt.com.config.Config;

public class NettyServer {
	public static int counter=0;
	public static void main(String[] args) throws Exception {
	
		// override default configuration parameters
		if (args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].startsWith("nettyport")) {
					Config.nettyPort = Integer.parseInt(args[i].split("=")[1]);
				}if (args[i].startsWith("tarantoolport")) {
					Config.tarantoolPort = Integer.parseInt(args[i].split("=")[1]);
				}else if (args[i].startsWith("address")) {
					Config.address = args[i].split("=")[1];
				} else if (args[i].startsWith("user")) {
					Config.username = args[i].split("=")[1];
				} else if (args[i].startsWith("password")) {
					Config.password = args[i].split("=")[1];
				} else if (args[i].startsWith("campaignDailySpendsID")) {
					Config.campaignDailySpendsID = Integer.parseInt(args[i].split("=")[1]);
				} else if (args[i].startsWith("advertiserLedgerID")) {
					Config.advertiserLedgerID = Integer.parseInt(args[i].split("=")[1]);
				} else if (args[i].startsWith("advertiserCamapignMapID")) {
					Config.advertiserCamapignMapID = Integer.parseInt(args[i].split("=")[1]);
				} 
			}
		}

		final SslContext sslCtx = null;

		// Configure the server.
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
					.handler(new LoggingHandler(LogLevel.INFO)).childHandler(new NettyServerInitializer(sslCtx));
			
			
			
			Channel ch = b.bind(Config.nettyPort).sync().channel();

			System.err.println("Open your web browser and navigate to http://127.0.0.1:" + Config.nettyPort + '/');

			ch.closeFuture().sync();
		} finally {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
}

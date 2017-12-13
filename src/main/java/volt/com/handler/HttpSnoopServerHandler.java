package volt.com.handler;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import javax.activation.MimetypesFileTypeMap;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;
import volt.com.client.ExecuteQuery;
import volt.com.server.NettyServer;

public class HttpSnoopServerHandler extends SimpleChannelInboundHandler<Object> {
	public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
	public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
	public static final int HTTP_CACHE_SECONDS = 60;

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws URISyntaxException {

		// code to get parameters from the request url
		if (msg instanceof HttpRequest) {
			HttpRequest request = (HttpRequest) msg;

			String uri = request.uri();
			String filename = "loaderio-fbe3a1e6ef2f4ae69f460c0ae454538b.txt";
			if (uri.endsWith(filename)) {
				try {

					File file = new File("../../../" + filename);
					RandomAccessFile raf;

					raf = new RandomAccessFile(file, "r");
					long fileLength = raf.length();
					HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
					HttpUtil.setContentLength(response, fileLength);
					setContentTypeHeader(response, file);
					setDateAndCacheHeaders(response, file);
					if (HttpUtil.isKeepAlive(request)) {
						response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
					}

					// Write the initial line and the header.
					ctx.write(response);

					ChannelFuture sendFileFuture;
					ChannelFuture lastContentFuture;
					if (ctx.pipeline().get(SslHandler.class) == null) {
						sendFileFuture = ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength),
								ctx.newProgressivePromise());
						// Write the end marker.
						lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
					} else {
						sendFileFuture = ctx.writeAndFlush(
								new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)),
								ctx.newProgressivePromise());
						// HttpChunkedInput will write the end marker
						// (LastHttpContent) for us.
						lastContentFuture = sendFileFuture;
					}

					sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
						public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
							if (total < 0) { // total unknown
								System.err.println(future.channel() + " Transfer progress: " + progress);
							} else {
								System.err
										.println(future.channel() + " Transfer progress: " + progress + " / " + total);
							}
						}

						public void operationComplete(ChannelProgressiveFuture future) {
							System.err.println(future.channel() + " Transfer complete.");
						}
					});

				} catch (FileNotFoundException ignore) {
					ignore.printStackTrace();
					sendError(ctx, NOT_FOUND);

					return;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} else {
				QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
				Map<String, List<String>> params = queryStringDecoder.parameters();

			}
		}
		NettyServer.counter++;
		new ExecuteQuery().getDetailedCampaign(NettyServer.counter);

	}

	private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
		FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status,
				Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
		response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

		// Close the connection as soon as the error message is sent.
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}

	private static void setDateAndCacheHeaders(HttpResponse response, File fileToCache) {
		SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
		dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

		// Date header
		Calendar time = new GregorianCalendar();
		response.headers().set(HttpHeaderNames.DATE, dateFormatter.format(time.getTime()));

		// Add cache headers
		time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
		response.headers().set(HttpHeaderNames.EXPIRES, dateFormatter.format(time.getTime()));
		response.headers().set(HttpHeaderNames.CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
		response.headers().set(HttpHeaderNames.LAST_MODIFIED,
				dateFormatter.format(new Date(fileToCache.lastModified())));
	}

	private static void setContentTypeHeader(HttpResponse response, File file) {
		MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
		response.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

}
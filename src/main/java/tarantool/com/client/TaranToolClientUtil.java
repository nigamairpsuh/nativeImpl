package tarantool.com.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;

import tarantool.com.config.Config;

public class TaranToolClientUtil {

	private static TarantoolClient client;

	static long totalResponseTime = 0; //keep adding execution time of each request
	static int countofRequests = 0; //number of requests executed at any given moment
//	averageResponseTime = totalResponseTime/countofRequests
	
//	queries executed inside GetDetailedCampaignBalance procedure
	static String campaignTransactions = "airpushdb.ViewList().model().vw_with_nfr_aggr_campaign_daily_transactions("
			+ Config.advertiserId + "," + Config.campaignId + "," + Config.balanceDate + ")";

	static String advertiserLedger = "airpushdb.ViewList().model().vw_aggr_advertiser_ledger_balance("
			+ Config.advertiserId + ")";
	
	static  String advertiserDailySpends = "airpushdb.ViewList().model().vw_with_nfr_aggr_advertiser_daily_spends("
			+ Config.advertiserId + "," + Config.balanceDate + ")";
	
	static  String advertiserCampaignMap =
	 "airpushdb.TableList().Advertiser_campaign_map().model().select_by_advertiser_id_campaign_id("
	 + Config.advertiserId + "," + Config.campaignId + ")";
	
	
	static FileWriter fw = null;
	static BufferedWriter bw = null;
	static {
		try {
			fw = new FileWriter("loadResponse_" + Config.logName + ".log", true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
	}

//	native implementation of GetDetailedCampaignBalance procedure
	public static void getDetailedCampaign() {

		try {
			TarantoolClient client = getClient();

			bw.write("Executing native SP implementation\n");
			System.out.println("Executing Native SP implementation\n");
		

			long startTime = System.currentTimeMillis();
			

			// sync operations
			// List<?> res1 = client.syncOps().eval(campaignTransactions, 1);
			// bw.write("vw_with_nfr_aggr_campaign_daily_transactions response
			// \t after " + (System.currentTimeMillis() - startTime) + "\n");
			// List<?> res2 = client.syncOps().eval(advertiserLedger , 1);
			// bw.write("vw_aggr_advertiser_ledger_balance response \t after " +
			// (System.currentTimeMillis() - startTime) + "\n");
			// List<?> res3 = client.syncOps().eval(advertiserDailySpends , 1);
			// bw.write("vw_with_nfr_aggr_advertiser_daily_spends response \t
			// after " + (System.currentTimeMillis() - startTime) + "\n");
			// List<?> res4 = client.syncOps().eval(advertiserCampaignMap , 1);
			// bw.write("Advertiser_campaign_map response \t after " +
			// (System.currentTimeMillis() - startTime) + "\n");

			
//			ToDo- check why empty response is obtained in java client
//			Note- java client is able to execute procedures but response obtained at client side is empty
			// async operations
			Future<List<?>> campaignTransactionsResult = client.asyncOps().eval(campaignTransactions, 1);
			Future<List<?>> advertiserLedgerResult = client.asyncOps().eval(advertiserLedger, 1);
			Future<List<?>> advertiserDailySpendsResult = client.asyncOps().eval(advertiserDailySpends, 1);
			Future<List<?>> advertiserCampaignMapResult = client.asyncOps().eval(advertiserCampaignMap, 1);

			try {
				campaignTransactionsResult.get();
				bw.write("vw_with_nfr_aggr_campaign_daily_transactions response \t after "
						+ (System.currentTimeMillis() - startTime) + "\n");

				advertiserLedgerResult.get();
				bw.write("vw_aggr_advertiser_ledger_balance response  \t after "
						+ (System.currentTimeMillis() - startTime) + "\n");

				advertiserDailySpendsResult.get();
				bw.write("vw_with_nfr_aggr_advertiser_daily_spends response  \t after "
						+ (System.currentTimeMillis() - startTime) + "\n");

				advertiserCampaignMapResult.get();
				bw.write("Advertiser_campaign_map response  \t after " + (System.currentTimeMillis() - startTime)
						+ "\n");

			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			long endTime = System.currentTimeMillis();
			System.out.println("time took in executing Native SP implementation " + (endTime - startTime));
			bw.write("time took in executing Native SP implementation " + (endTime - startTime) + "\n");

			countofRequests++;
			totalResponseTime = totalResponseTime + (endTime - startTime);
			bw.write("Average Response Time till " + countofRequests + " requests is "
					+ (totalResponseTime / countofRequests));
			bw.write("\n**********************************\n\n\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		finally {
			try {
				// not closing the stream in order to use it to log other
				// requests
				if (bw != null)
					bw.flush();

				if (fw != null)
					fw.flush();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}
	}

	public static void main(String[] args) {
		TarantoolClient client = getClient();
		client.syncOps().eval(campaignTransactions, 1);
		client.syncOps().eval(advertiserLedger, 1);
		client.syncOps().eval(advertiserDailySpends, 1);
		client.syncOps().eval(advertiserCampaignMap, 1);

	}

	static TarantoolClient getClient() {
		if (client != null)
			return client;

		TarantoolClientConfig config = new TarantoolClientConfig();
		config.username = Config.username;
		config.password = Config.password;
		config.initTimeoutMillis = 60000;

		SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
			public SocketChannel get(int retryNumber, Throwable lastError) {
				if (lastError != null) {
					lastError.printStackTrace(System.out);
				}
				try {
					return SocketChannel.open(new InetSocketAddress(Config.address, Config.tarantoolPort));
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}
		};
		return new TarantoolClientImpl(socketChannelProvider, config);
	}

}
